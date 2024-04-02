// Package repl contains binary log subscription functionality.
package repl

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/siddontang/loggers"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/cashapp/spirit/pkg/dbconn"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/cashapp/spirit/pkg/utils"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const (
	binlogTrivialThreshold = 10000
	// DefaultBatchSize is the number of rows in each batched REPLACE/DELETE statement.
	// Larger is better, but we need to keep the run-time of the statement well below
	// dbconn.maximumLockTime so that it doesn't prevent copy-row tasks from failing.
	// Since on some of our Aurora tables with out-of-cache workloads only copy ~300 rows per second,
	// we probably shouldn't set this any larger than about 1K. It will also use
	// multiple-flush-threads, which should help it group commit and still be fast.
	// This is only used as an initial starting value. It will auto-scale based on the DefaultTargetBatchTime.
	DefaultBatchSize = 1000

	// DefaultTargetBatchTime is the target time for flushing REPLACE/DELETE statements.
	DefaultTargetBatchTime = time.Millisecond * 500

	// DefaultFlushInterval is the time that the client will flush all binlog changes to disk.
	// Longer values require more memory, but permit more merging.
	// I expect we will change this to 1hr-24hr in the future.
	DefaultFlushInterval = 30 * time.Second
)

type queuedChange struct {
	key      string
	isDelete bool
}

type statement struct {
	numKeys int
	stmt    string
}

func extractStmt(stmts []statement) []string {
	var trimmed []string
	for _, stmt := range stmts {
		if stmt.stmt != "" {
			trimmed = append(trimmed, stmt.stmt)
		}
	}
	return trimmed
}

type Client struct {
	canal.DummyEventHandler
	sync.Mutex
	host     string
	username string
	password string

	binlogChangeset      map[string]bool // bool is deleted
	binlogChangesetDelta int64           // a special "fix" for keys that have been popped off, use atomic get/set
	binlogPosSynced      mysql.Position  // safely written to new table
	binlogPosInMemory    mysql.Position  // available in the binlog binlogChangeset
	lastLogFileName      string          // last log file name we've seen in a rotation event

	queuedChanges []queuedChange // used when disableDeltaMap is true

	canal *canal.Canal

	changesetRowsCount      int64
	changesetRowsEventCount int64 // eliminated by optimizations

	db *sql.DB // connection to run queries like SHOW MASTER STATUS

	// Infoschema version of table.
	table    *table.TableInfo
	newTable *table.TableInfo

	enableKeyAboveWatermark bool
	disableDeltaMap         bool // use queue instead

	TableChangeNotificationCallback func()
	KeyAboveCopierCallback          func(interface{}) bool

	isClosed bool

	statisticsLock  sync.Mutex
	targetBatchTime time.Duration
	targetBatchSize int64 // will auto-adjust over time, use atomic to read/set
	timingHistory   []time.Duration
	concurrency     int

	// The periodic flush lock is just used for ensuring only one periodic flush runs at a time,
	// and when we disable it, no more periodic flushes will run. The actual flushing is protected
	// by a lower level lock (sync.Mutex on Client)
	periodicFlushLock    sync.Mutex
	periodicFlushEnabled bool

	logger loggers.Advanced
}

func NewClient(db *sql.DB, host string, table, newTable *table.TableInfo, username, password string, config *ClientConfig) *Client {
	return &Client{
		db:              db,
		host:            host,
		table:           table,
		newTable:        newTable,
		username:        username,
		password:        password,
		binlogChangeset: make(map[string]bool),
		logger:          config.Logger,
		targetBatchTime: config.TargetBatchTime,
		targetBatchSize: DefaultBatchSize, // initial starting value.
		concurrency:     config.Concurrency,
	}
}

type ClientConfig struct {
	TargetBatchTime time.Duration
	Concurrency     int
	Logger          loggers.Advanced
}

// NewClientDefaultConfig returns a default config for the copier.
func NewClientDefaultConfig() *ClientConfig {
	return &ClientConfig{
		Concurrency:     4,
		TargetBatchTime: DefaultTargetBatchTime,
		Logger:          logrus.New(),
	}
}

// OnRow is called when a row is discovered via replication.
// The event is of type e.Action and contains one
// or more rows in e.Rows. We find the PRIMARY KEY of the row:
// 1) If it exceeds the known high watermark of the copier we throw it away.
// (we've not copied that data yet - it will be already up to date when we copy it later).
// 2) If it could have been copied already, we add it to the changeset.
// We only need to add the PK + if the operation was a delete.
// This will be used after copy rows to apply any changes that have been made.
func (c *Client) OnRow(e *canal.RowsEvent) error {
	var i = 0
	for _, row := range e.Rows {
		// For UpdateAction there is always a before and after image (i.e. e.Rows is always in pairs.)
		// We only need to capture one of the events, and since in MINIMAL RBR row
		// image the PK is only included in the before, we chose that one.
		if e.Action == canal.UpdateAction {
			i++
			if i%2 == 0 {
				continue
			}
		}
		key, err := c.table.PrimaryKeyValues(row)
		if err != nil {
			return err
		}
		if len(key) == 0 {
			return fmt.Errorf("no primary key found for row: %#v", row)
		}
		atomic.AddInt64(&c.changesetRowsEventCount, 1)

		// The KeyAboveWatermark optimization has to be enabled
		// We enable it once all the setup has been done (since we create a repl client
		// earlier in setup to ensure binary logs are available).
		// We then disable the optimization after the copier phase has finished.
		if c.KeyAboveWatermarkEnabled() && c.KeyAboveCopierCallback(key[0]) {
			c.logger.Debugf("key above watermark: %v", key[0])
			continue // key can be ignored
		}
		switch e.Action {
		case canal.InsertAction, canal.UpdateAction:
			c.keyHasChanged(key, false)
		case canal.DeleteAction:
			c.keyHasChanged(key, true)
		default:
			c.logger.Errorf("unknown action: %v", e.Action)
		}
	}
	c.updatePosInMemory(e.Header.LogPos)
	return nil
}

// KeyAboveWatermarkEnabled returns true if the key above watermark optimization is enabled.
// and it's also safe to do so.
func (c *Client) KeyAboveWatermarkEnabled() bool {
	c.Lock()
	defer c.Unlock()
	return c.enableKeyAboveWatermark && c.KeyAboveCopierCallback != nil
}

// OnRotate is called when a rotate event is discovered via replication.
// We use this to capture the log file name, since only the position is caught on the row event.
func (c *Client) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	c.Lock()
	defer c.Unlock()
	c.lastLogFileName = string(rotateEvent.NextLogName)
	return nil
}

// OnTableChanged is called when a table is changed via DDL.
// This is a failsafe because we don't expect DDL to be performed on the table while we are operating.
func (c *Client) OnTableChanged(header *replication.EventHeader, schema string, table string) error {
	if (c.table.SchemaName == schema && c.table.TableName == table) ||
		(c.newTable.SchemaName == schema && c.newTable.TableName == table) {
		if c.TableChangeNotificationCallback != nil {
			c.TableChangeNotificationCallback()
		}
	}
	return nil
}

func (c *Client) SetKeyAboveWatermarkOptimization(newVal bool) {
	c.Lock()
	defer c.Unlock()

	c.enableKeyAboveWatermark = newVal
}

// SetPos is used for resuming from a checkpoint.
func (c *Client) SetPos(pos mysql.Position) {
	c.Lock()
	defer c.Unlock()
	c.binlogPosSynced = pos
}

func (c *Client) AllChangesFlushed() bool {
	if c.GetDeltaLen() > 0 {
		return false
	}
	c.Lock()
	defer c.Unlock()
	return c.binlogPosInMemory.Compare(c.binlogPosSynced) == 0
}

func (c *Client) GetBinlogApplyPosition() mysql.Position {
	c.Lock()
	defer c.Unlock()
	return c.binlogPosSynced
}

func (c *Client) GetDeltaLen() int {
	c.Lock()
	defer c.Unlock()
	if c.disableDeltaMap {
		return len(c.queuedChanges)
	}

	return len(c.binlogChangeset) + int(atomic.LoadInt64(&c.binlogChangesetDelta))
}

// pksToRowValueConstructor constructs a statement like this:
// DELETE FROM x WHERE (s_i_id,s_w_id) in ((7,10),(1,5));
func (c *Client) pksToRowValueConstructor(d []string) string {
	var pkValues []string
	for _, v := range d {
		pkValues = append(pkValues, utils.UnhashKey(v))
	}
	return strings.Join(pkValues, ",")
}

func (c *Client) getCurrentBinlogPosition() (mysql.Position, error) {
	var binlogFile, fake string
	var binlogPos uint32
	err := c.db.QueryRow("SHOW MASTER STATUS").Scan(&binlogFile, &binlogPos, &fake, &fake, &fake) //nolint: execinquery
	if err != nil {
		return mysql.Position{}, err
	}
	return mysql.Position{
		Name: binlogFile,
		Pos:  binlogPos,
	}, nil
}

func (c *Client) Run() (err error) {
	// We have to disable the delta map
	// if the primary key is *not* memory comparable.
	// We use a FIFO queue instead.
	if err := c.table.PrimaryKeyIsMemoryComparable(); err != nil {
		c.disableDeltaMap = true
	}
	cfg := canal.NewDefaultConfig()
	cfg.Addr = c.host
	cfg.User = c.username
	cfg.Password = c.password
	cfg.Logger = NewLogWrapper(c.logger) // wrapper to filter the noise.
	cfg.IncludeTableRegex = []string{fmt.Sprintf("^%s\\.%s$", c.table.SchemaName, c.table.TableName)}
	cfg.Dump.ExecutionPath = "" // skip dump
	if dbconn.IsRDSHost(cfg.Addr) {
		// create a new TLSConfig for RDS
		// It needs to be a copy because sharing a global pointer
		// is not thread safe when spirit is used as a library.
		cfg.TLSConfig = dbconn.NewTLSConfig()
		cfg.TLSConfig.ServerName = utils.StripPort(cfg.Addr)
	}
	c.canal, err = canal.NewCanal(cfg)
	if err != nil {
		return err
	}

	// The handle RowsEvent just writes to the migrators changeset buffer.
	// Which blocks when it needs to be emptied.
	c.canal.SetEventHandler(c)
	// All we need to do synchronously is get a position before
	// the table migration starts. Then we can start copying data.
	if c.binlogPosSynced.Name == "" {
		c.binlogPosSynced, err = c.getCurrentBinlogPosition()
		if err != nil {
			return errors.New("failed to get binlog position, check binary is enabled")
		}
	} else if c.binlogPositionIsImpossible() {
		// Canal needs to be called as a go routine, so before we do check that the binary log
		// Position is not impossible so we can return a synchronous error.
		return errors.New("binlog position is impossible, the source may have already purged it")
	}

	c.binlogPosInMemory = c.binlogPosSynced
	c.lastLogFileName = c.binlogPosInMemory.Name

	// Call start canal as a go routine.
	go c.startCanal()
	return nil
}

func (c *Client) binlogPositionIsImpossible() bool {
	rows, err := c.db.Query("SHOW MASTER LOGS") //nolint: execinquery
	if err != nil {
		return true // if we can't get the logs, its already impossible
	}
	defer rows.Close()

	var logname, size, encrypted string
	for rows.Next() {
		if err := rows.Scan(&logname, &size, &encrypted); err != nil {
			return true
		}
		if logname == c.binlogPosSynced.Name {
			return false // We just need presence of the log file for success
		}
	}
	if rows.Err() != nil {
		return true // can't determine.
	}
	return true
}

// Called as a go routine.
func (c *Client) startCanal() {
	// Start canal as a routine
	c.Lock()
	position := c.binlogPosSynced // avoid a data race, binlogPosSynced is always under mutex
	c.Unlock()
	c.logger.Debugf("starting binary log subscription. log-file: %s log-pos: %d", position.Name, position.Pos)
	if err := c.canal.RunFrom(position); err != nil {
		// Canal has failed! In future we might be able to reconnect and resume
		// if canal does not do so itself. For now, we just fail the migration
		// since we can resume from checkpoint anyway.
		if c.isClosed {
			// this is probably a replication.ErrSyncClosed error
			// but since canal is now closed we can safely return
			return
		}
		c.logger.Errorf("canal has failed. error: %v", err)
		panic("canal has failed")
	}
}

func (c *Client) Close() {
	c.Lock()
	defer c.Unlock()
	c.isClosed = true
	if c.canal != nil {
		c.canal.Close()
	}
}

func (c *Client) updatePosInMemory(pos uint32) {
	c.Lock()
	defer c.Unlock()
	c.binlogPosInMemory = mysql.Position{
		Name: c.lastLogFileName,
		Pos:  pos,
	}
}

// FlushUnderLock is a final flush under an exclusive lock using the connection
// that holds a write lock.
func (c *Client) FlushUnderLock(ctx context.Context, lock *dbconn.TableLock) error {
	return c.flush(ctx, true, lock)
}

func (c *Client) flush(ctx context.Context, underLock bool, lock *dbconn.TableLock) error {
	if c.disableDeltaMap {
		return c.flushQueue(ctx, underLock, lock)
	}
	return c.flushMap(ctx, underLock, lock)
}

// flushQueue flushes the FIFO queue that is used when the PRIMARY KEY
// is not memory comparable. It needs to be single threaded,
// so it might not scale as well as the Delta Map, but offering
// it at least helps improve compatibility.
//
// The only optimization we do is we try to MERGE statements together, such
// that if there are operations: REPLACE<1>, REPLACE<2>, DELETE<3>, REPLACE<4>
// we merge it to REPLACE<1,2>, DELETE<3>, REPLACE<4>.
func (c *Client) flushQueue(ctx context.Context, underLock bool, lock *dbconn.TableLock) error {
	c.Lock()
	changesToFlush := c.queuedChanges
	c.queuedChanges = nil // reset
	posOfFlush := c.binlogPosInMemory
	c.Unlock()

	// Early return if there is nothing to flush.
	if len(changesToFlush) == 0 {
		c.SetPos(posOfFlush)
		return nil
	}

	// Otherwise, flush the changes.
	var stmts []statement
	var buffer []string
	prevKey := changesToFlush[0] // for initialization
	target := int(atomic.LoadInt64(&c.targetBatchSize))
	for _, change := range changesToFlush {
		// We are changing from DELETE to REPLACE
		// or vice versa, *or* the buffer is getting very large.
		if change.isDelete != prevKey.isDelete || len(buffer) > target {
			if prevKey.isDelete {
				stmts = append(stmts, c.createDeleteStmt(buffer))
			} else {
				stmts = append(stmts, c.createReplaceStmt(buffer))
			}
			buffer = nil // reset
		}
		buffer = append(buffer, change.key)
		prevKey.isDelete = change.isDelete
	}
	// Flush the buffer once more.
	if prevKey.isDelete {
		stmts = append(stmts, c.createDeleteStmt(buffer))
	} else {
		stmts = append(stmts, c.createReplaceStmt(buffer))
	}
	if underLock {
		// Execute under lock means it is a final flush
		// We need to use the lock connection to do this
		// so there is no parallelism.
		if err := lock.ExecUnderLock(ctx, extractStmt(stmts)); err != nil {
			return err
		}
	} else {
		// Execute the statements in a transaction.
		// They still need to be single threaded.
		if _, err := dbconn.RetryableTransaction(ctx, c.db, true, dbconn.NewDBConfig(), extractStmt(stmts)...); err != nil {
			return err
		}
	}
	c.SetPos(posOfFlush)
	return nil
}

// flushMap is the internal version of Flush() for the delta map.
// it is used by default unless the PRIMARY KEY is non memory comparable.
func (c *Client) flushMap(ctx context.Context, underLock bool, lock *dbconn.TableLock) error {
	c.Lock()
	setToFlush := c.binlogChangeset
	posOfFlush := c.binlogPosInMemory         // copy the value, not the pointer
	c.binlogChangeset = make(map[string]bool) // set new value
	c.Unlock()                                // unlock immediately so others can write to the changeset
	// The changeset delta is because the status output is based on len(binlogChangeset)
	// which just got reset to zero. We need some way to communicate roughly in status output
	// there is other pending work while this func is running. We'll reset the delta
	// to zero when this func exits.
	atomic.StoreInt64(&c.binlogChangesetDelta, int64(len(setToFlush)))

	defer func() {
		atomic.AddInt64(&c.changesetRowsCount, int64(len(setToFlush)))
		atomic.StoreInt64(&c.binlogChangesetDelta, int64(0)) // reset the delta
	}()

	// We must now apply the changeset setToFlush to the new table.
	var deleteKeys []string
	var replaceKeys []string
	var stmts []statement
	var i int64
	target := atomic.LoadInt64(&c.targetBatchSize)
	for key, isDelete := range setToFlush {
		i++
		if isDelete {
			deleteKeys = append(deleteKeys, key)
		} else {
			replaceKeys = append(replaceKeys, key)
		}
		if (i % target) == 0 {
			stmts = append(stmts, c.createDeleteStmt(deleteKeys))
			stmts = append(stmts, c.createReplaceStmt(replaceKeys))
			deleteKeys = []string{}
			replaceKeys = []string{}
			atomic.AddInt64(&c.binlogChangesetDelta, -target)
		}
	}
	stmts = append(stmts, c.createDeleteStmt(deleteKeys))
	stmts = append(stmts, c.createReplaceStmt(replaceKeys))

	if underLock {
		// Execute under lock means it is a final flush
		// We need to use the lock connection to do this
		// so there is no parallelism.
		if err := lock.ExecUnderLock(ctx, extractStmt(stmts)); err != nil {
			return err
		}
	} else {
		// Execute the statements in parallel
		// They should not conflict and order should not matter
		// because they come from a consistent view of a map,
		// which is distinct keys.
		g, errGrpCtx := errgroup.WithContext(ctx)
		g.SetLimit(c.concurrency)
		for _, stmt := range stmts {
			s := stmt
			g.Go(func() error {
				startTime := time.Now()
				_, err := dbconn.RetryableTransaction(errGrpCtx, c.db, false, dbconn.NewDBConfig(), s.stmt)
				c.feedback(s.numKeys, time.Since(startTime))
				return err
			})
		}
		// wait for all work to finish
		if err := g.Wait(); err != nil {
			return err
		}
	}
	// Update the synced binlog position to the posOfFlush
	// uses a mutex.
	c.SetPos(posOfFlush)
	return nil
}

func (c *Client) createDeleteStmt(deleteKeys []string) statement {
	var deleteStmt string
	if len(deleteKeys) > 0 {
		deleteStmt = fmt.Sprintf("DELETE FROM %s WHERE (%s) IN (%s)",
			c.newTable.QuotedName,
			table.QuoteColumns(c.table.KeyColumns),
			c.pksToRowValueConstructor(deleteKeys),
		)
	}
	return statement{
		numKeys: len(deleteKeys),
		stmt:    deleteStmt,
	}
}

func (c *Client) createReplaceStmt(replaceKeys []string) statement {
	var replaceStmt string
	if len(replaceKeys) > 0 {
		replaceStmt = fmt.Sprintf("REPLACE INTO %s (%s) SELECT %s FROM %s FORCE INDEX (PRIMARY) WHERE (%s) IN (%s)",
			c.newTable.QuotedName,
			utils.IntersectColumns(c.table, c.newTable),
			utils.IntersectColumns(c.table, c.newTable),
			c.table.QuotedName,
			table.QuoteColumns(c.table.KeyColumns),
			c.pksToRowValueConstructor(replaceKeys),
		)
	}
	return statement{
		numKeys: len(replaceKeys),
		stmt:    replaceStmt,
	}
}

// feedback provides feedback on the apply time of changesets.
// We use this to refine the targetBatchSize. This is a little bit
// different for feedback for the copier, because frequently the batches
// will not be full. We still need to use a p90-like mechanism though,
// because the rows being changed are by definition more likely to be hotspots.
// Hotspots == Lock Contention. This is one of the exact reasons why we are
// chunking in the first place. The probability that the applier can cause
// impact on OLTP workloads is much higher than the copier.
func (c *Client) feedback(numberOfKeys int, d time.Duration) {
	c.statisticsLock.Lock()
	defer c.statisticsLock.Unlock()
	if numberOfKeys == 0 {
		// If the number of keys is zero, we can't
		// calculate anything so we just return
		return
	}
	// For the p90-like mechanism rather than storing all the previous
	// durations, because the numberOfKeys is variable we instead store
	// the timePerKey. We then adjust the targetBatchSize based on this.
	// This creates some skew because small batches will have a higher
	// timePerKey, which can create a back log. Which results in a smaller
	// timePerKey. So at least the skew *should* be self-correcting. This
	// has not yet been proven though.
	timePerKey := d / time.Duration(numberOfKeys)
	c.timingHistory = append(c.timingHistory, timePerKey)

	// If we have enough feedback re-evaluate the target batch size
	// based on the p90 timePerKey.
	if len(c.timingHistory) >= 10 {
		timePerKey := table.LazyFindP90(c.timingHistory)
		newBatchSize := int64(float64(c.targetBatchTime) / float64(timePerKey))
		atomic.StoreInt64(&c.targetBatchSize, newBatchSize)
		c.timingHistory = nil // reset
	}
}

// Flush empties the changeset in a loop until the amount of changes is considered "trivial".
// The loop is required, because changes continue to be added while the flush is occurring.
func (c *Client) Flush(ctx context.Context) error {
	c.logger.Info("starting to flush changeset")
	for {
		// Repeat in a loop until the changeset length is trivial
		if err := c.flush(ctx, false, nil); err != nil {
			return err
		}
		// Wait for canal to catch up before determining if the changeset
		// length is considered trivial.
		if err := c.BlockWait(ctx); err != nil {
			return err
		}
		if c.GetDeltaLen() < binlogTrivialThreshold {
			break
		}
	}
	// Flush one more time, since after BlockWait()
	// there might be more changes.
	if err := c.flush(ctx, false, nil); err != nil {
		return err
	}
	return nil
}

// StopPeriodicFlush disables the periodic flush, also guaranteeing
// when it returns there is no current flush running
func (c *Client) StopPeriodicFlush() {
	c.periodicFlushLock.Lock()
	defer c.periodicFlushLock.Unlock()
	c.periodicFlushEnabled = false
}

// StartPeriodicFlush starts a loop that periodically flushes the binlog changeset.
// This is used by the migrator to ensure the binlog position is advanced.
func (c *Client) StartPeriodicFlush(ctx context.Context, interval time.Duration) {
	c.periodicFlushLock.Lock()
	c.periodicFlushEnabled = true
	c.periodicFlushLock.Unlock()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.periodicFlushLock.Lock()
			// At some point before cutover we want to disable th periodic flush.
			// The migrator will do this by calling StopPeriodicFlush()
			if !c.periodicFlushEnabled {
				c.periodicFlushLock.Unlock()
				return
			}
			startLoop := time.Now()
			c.logger.Debug("starting periodic flush of binary log")
			// The periodic flush does not respect the throttler since we want to advance the binlog position
			// we allow this to run, and then expect that if it is under load the throttler
			// will kick in and slow down the copy-rows.
			if err := c.flush(ctx, false, nil); err != nil {
				c.logger.Errorf("error flushing binary log: %v", err)
			}
			c.periodicFlushLock.Unlock()
			c.logger.Infof("finished periodic flush of binary log: total-duration=%v batch-size=%d",
				time.Since(startLoop),
				atomic.LoadInt64(&c.targetBatchSize),
			)
		}
	}
}

// BlockWait blocks until the *canal position* has caught up to the current binlog position.
// This is usually called by Flush() which then ensures the changes are flushed.
// Calling it directly is usually only used by the test-suite!
// There is a built-in func in canal to do this, but it calls FLUSH BINARY LOGS,
// which requires additional permissions.
// **Caveat** Unless you are calling this from Flush(), calling this DOES NOT ensure that
// changes have been applied to the database.
func (c *Client) BlockWait(ctx context.Context) error {
	targetPos, err := c.canal.GetMasterPos() // what the server is at.
	if err != nil {
		return err
	}
	for i := 100; ; i++ {
		if err := c.injectBinlogNoise(ctx); err != nil {
			return err
		}
		canalPos := c.canal.SyncedPosition()
		if i%100 == 0 {
			// Print status every 100 loops = 10s
			c.logger.Infof("blocking until we have read all binary logs: current-pos=%s target-pos=%s", canalPos, targetPos)
		}
		if canalPos.Compare(targetPos) >= 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

// injectBinlogNoise is used to inject some noise into the binlog stream
// This helps ensure that we are "past" a binary log position if there is some off-by-one
// problem where the most recent canal event is not yet updating the canal SyncedPosition,
// and there are no current changes on the MySQL server to advance itself.
// Note: We can not update the table or the newTable, because this intentionally
// causes a panic (c.tableChanged() is called).
func (c *Client) injectBinlogNoise(ctx context.Context) error {
	tblName := fmt.Sprintf("_%s_chkpnt", c.table.TableName)
	return dbconn.Exec(ctx, c.db, "ALTER TABLE %n.%n AUTO_INCREMENT=0", c.table.SchemaName, tblName)
}

func (c *Client) keyHasChanged(key []interface{}, deleted bool) {
	c.Lock()
	defer c.Unlock()

	if c.disableDeltaMap {
		c.queuedChanges = append(c.queuedChanges, queuedChange{key: utils.HashKey(key), isDelete: deleted})
		return
	}
	c.binlogChangeset[utils.HashKey(key)] = deleted
}

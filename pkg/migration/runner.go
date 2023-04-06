package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/loggers"
	"github.com/sirupsen/logrus"
	"github.com/squareup/spirit/pkg/check"
	"github.com/squareup/spirit/pkg/checksum"
	"github.com/squareup/spirit/pkg/dbconn"
	"github.com/squareup/spirit/pkg/repl"
	"github.com/squareup/spirit/pkg/row"
	"github.com/squareup/spirit/pkg/table"
	"github.com/squareup/spirit/pkg/throttler"
)

type migrationState int32

const (
	stateInitial migrationState = iota
	stateCopyRows
	stateApplyChangeset // first mass apply
	stateAnalyzeTable
	stateChecksum
	statePostChecksum // second mass apply
	stateCutOver
	stateClose
	stateErrCleanup
)

const (
	checkpointDumpInterval  = 50 * time.Second
	tableStatUpdateInterval = 30 * time.Second
	statusInterval          = 10 * time.Second
	copyEstimateInterval    = 5 * time.Second
	// binlogPerodicFlushInterval is the time that the client will flush all binlog changes to disk.
	// Longer values require more memory, but permit more merging.
	// I expect we will change this to 1hr-24hr in future.
	binlogPerodicFlushInterval = 30 * time.Second
)

func (s migrationState) String() string {
	switch s {
	case stateInitial:
		return "initial"
	case stateCopyRows:
		return "copyRows"
	case stateApplyChangeset:
		return "applyChangeset"
	case stateAnalyzeTable:
		return "analyzeTable"
	case stateChecksum:
		return "checksum"
	case statePostChecksum:
		return "postChecksum"
	case stateCutOver:
		return "cutOver"
	case stateClose:
		return "close"
	case stateErrCleanup:
		return "errCleanup"
	}
	return "unknown"
}

type Runner struct {
	host           string
	username       string
	password       string
	tableName      string
	schemaName     string
	alterStatement string

	db              *sql.DB
	replica         *sql.DB
	table           *table.TableInfo
	newTable        *table.TableInfo
	checkpointTable *table.TableInfo

	currentState migrationState // must use atomic to get/set
	replClient   *repl.Client   // feed contains all binlog subscription activity.
	copier       *row.Copier
	throttler    throttler.Throttler
	checker      *checksum.Checker

	// Track some key statistics.
	startTime time.Time

	// Used by the test-suite and some post-migration output.
	// Indicates if certain optimizations applied.
	usedInstantDDL bool
	usedInplaceDDL bool

	// Configurable Options that might be passed in
	// defaults will be set in NewRunner()
	optConcurrency           int
	optChecksumConcurrency   int
	optTargetChunkTime       time.Duration
	optAttemptInplaceDDL     bool
	optChecksum              bool
	optDisableTrivialChunker bool
	optReplicaDSN            string
	optReplicaMaxLag         time.Duration

	// We want to block periodic flushes while we're doing a checksums etc
	periodicFlushLock sync.Mutex

	// Attached logger
	logger loggers.Advanced
}

func NewRunner(migration *Migration) (*Runner, error) {
	m := &Runner{
		host:                     migration.Host,
		username:                 migration.Username,
		password:                 migration.Password,
		schemaName:               migration.Database,
		tableName:                migration.Table,
		alterStatement:           migration.Alter,
		optConcurrency:           migration.Concurrency,
		optChecksumConcurrency:   migration.ChecksumConcurrency,
		optTargetChunkTime:       migration.TargetChunkTime,
		optAttemptInplaceDDL:     migration.AttemptInplaceDDL,
		optChecksum:              migration.Checksum,
		optDisableTrivialChunker: migration.DisableTrivialChunker,
		optReplicaDSN:            migration.ReplicaDSN,
		optReplicaMaxLag:         migration.ReplicaMaxLag,
		logger:                   logrus.New(),
	}
	if m.optTargetChunkTime == 0 {
		m.optTargetChunkTime = 100 * time.Millisecond
	}
	if m.optConcurrency == 0 {
		m.optConcurrency = 4
	}
	if m.optChecksumConcurrency == 0 {
		m.optChecksumConcurrency = m.optConcurrency
	}
	if m.host == "" {
		return nil, errors.New("host is required")
	}
	if !strings.Contains(m.host, ":") {
		m.host = fmt.Sprintf("%s:%d", m.host, 3306)
	}
	if m.schemaName == "" {
		return nil, errors.New("schema name is required")
	}
	if m.tableName == "" {
		return nil, errors.New("table name is required")
	}
	if m.alterStatement == "" {
		return nil, errors.New("alter statement is required")
	}
	return m, nil
}

func (m *Runner) SetLogger(logger loggers.Advanced) {
	m.logger = logger
}

func (m *Runner) Run(ctx context.Context) error {
	m.startTime = time.Now()
	m.logger.Infof("Starting spirit migration: concurrency=%d target-chunk-size=%s table=%s.%s alter=\"%s\"",
		m.optConcurrency, m.optTargetChunkTime, m.schemaName, m.tableName, m.alterStatement,
	)

	// Create a database connection
	// It will be closed in m.Close()
	var err error
	m.db, err = sql.Open("mysql", m.dsn())
	if err != nil {
		return err
	}
	if err := m.db.Ping(); err != nil {
		return err
	}

	// Get Table Info
	m.table = table.NewTableInfo(m.db, m.schemaName, m.tableName)
	if err := m.table.SetInfo(ctx); err != nil {
		return err
	}
	// This step is technically optional, but first we attempt to
	// use MySQL's built-in DDL. This is because it's usually faster
	// when it is compatible. If it returns no error, that means it
	// has been successful and the DDL is complete.
	err = m.attemptMySQLDDL(ctx)
	if err == nil {
		m.logger.Infof("apply complete: instant-ddl=%v inplace-ddl=%v", m.usedInstantDDL, m.usedInplaceDDL)
		return nil // success!
	}

	// Perform preflight basic checks.
	if err := m.runChecks(ctx, check.ScopePreflight); err != nil {
		return err
	}

	// Perform setup steps, including resuming from a checkpoint (if available)
	// and creating the new and checkpoint tables.
	// The replication client is also created here.
	if err := m.setup(ctx); err != nil {
		return err
	}

	// Run post-setup checks
	if err := m.runChecks(ctx, check.ScopePostSetup); err != nil {
		return err
	}

	go m.estimateRowsPerSecondLoop()            // start estimating rows per second
	go m.dumpStatus()                           // start periodically writing status
	go m.dumpCheckpointContinuously(ctx)        // start periodically dumping the checkpoint.
	go m.updateTableStatisticsContinuously(ctx) // update the min/max and estimated rows.
	go m.periodicFlush(ctx)                     // advance the binary log position periodically.

	// Perform the main copy rows task. This is where the majority
	// of migrations usually spend time.
	m.setCurrentState(stateCopyRows)
	if err := m.copier.Run(ctx); err != nil {
		return err
	}
	m.logger.Info("copy rows complete")

	// Perform steps to prepare for final cutover.
	// This includes computing an optional checksum,
	// catching up on replClient apply, running ANALYZE TABLE so
	// that the statistics will be up to date on cutover.
	if err := m.prepareForCutover(ctx); err != nil {
		return err
	}
	// Run any checks that need to be done pre-cutover.
	if err := m.runChecks(ctx, check.ScopeCutover); err != nil {
		return err
	}
	// It's time for the final cut-over, where
	// the tables are swapped under a lock.
	m.setCurrentState(stateCutOver)
	cutover, err := NewCutOver(m.db, m.table, m.newTable, m.replClient, m.logger)
	if err != nil {
		return err
	}
	// Drop the _old table if it exists. This ensures
	// that the rename will succeed (although there is a brief race)
	if err := m.dropOldTable(ctx); err != nil {
		return err
	}
	if err := cutover.Run(ctx); err != nil {
		return err
	}

	checksumTime := time.Duration(0)
	if m.checker != nil {
		checksumTime = m.checker.ExecTime
	}
	m.logger.Infof("apply complete: instant-ddl=%v inplace-ddl=%v total-chunks=%v copy-rows-time=%s checksum-time=%s total-time=%s",
		m.usedInstantDDL,
		m.usedInplaceDDL,
		m.copier.CopyChunksCount,
		m.copier.ExecTime,
		checksumTime,
		time.Since(m.startTime),
	)
	return nil
}

// prepareForCutover performs steps to prepare for the final cutover.
// most of these steps are technically optional, but skipping them
// could for example cause a stall during the cutover if the replClient
// has too many pending updates.
func (m *Runner) prepareForCutover(ctx context.Context) error {
	// Recursively apply all pending events (FlushUntilTrivial)
	m.setCurrentState(stateApplyChangeset)
	m.periodicFlushLock.Lock() // Wait for the periodic flush to finish.
	if err := m.replClient.FlushUntilTrivial(ctx); err != nil {
		m.periodicFlushLock.Unlock() // need to yield before returning.
		return err
	}
	m.periodicFlushLock.Unlock() // It will not start again because the current state is now ApplyChangeset.

	// Run ANALYZE TABLE to update the statistics on the new table.
	// This is required so on cutover plans don't go sideways, which
	// is at elevated risk because the batch loading can cause statistics
	// to be out of date.
	m.setCurrentState(stateAnalyzeTable)
	stmt := fmt.Sprintf("ANALYZE TABLE %s", m.newTable.QuotedName())
	m.logger.Infof("Running: %s", stmt)
	if err := dbconn.DBExec(ctx, m.db, stmt); err != nil {
		return err
	}

	// The checksum is (usually) optional, but it is ONLINE after an initial lock
	// for consistency. It is the main way that we determine that
	// this program is safe to use even when immature. In the event that it is
	// a resume-from-checkpoint operation, the checksum is NOT optional.
	// This is because adding a unique index can not be differentiated from a
	// duplicate key error caused by retrying partial work.
	if m.optChecksum {
		if err := m.checksum(ctx); err != nil {
			return err
		}
	}
	return nil
}

// runChecks wraps around check.RunChecks and adds the context of this migration
func (m *Runner) runChecks(ctx context.Context, scope check.ScopeFlag) error {
	return check.RunChecks(ctx, check.Resources{
		DB:      m.db,
		Replica: m.replica,
		Table:   m.table,
		Alter:   m.alterStatement,
	}, m.logger, scope)
}

// attemptMySQLDDL "attempts" to use DDL directly on MySQL with an assertion
// such as ALGORITHM=INSTANT. If MySQL is able to use the INSTANT algorithm,
// it will perform the operation without error. If it can't, it will return
// an error. It is important to let MySQL decide if it can handle the DDL
// operation, because keeping track of which operations are "INSTANT"
// is incredibly difficult. It will depend on MySQL minor version,
// and could possibly be specific to the table.
func (m *Runner) attemptMySQLDDL(ctx context.Context) error {
	err := m.attemptInstantDDL(ctx)
	if err == nil {
		m.usedInstantDDL = true // success
		return nil
	}
	// Inplace DDL is feature gated because it blocks replicas.
	// It's only safe to do in aurora GLOBAL because replicas do not
	// use the binlog.
	if m.optAttemptInplaceDDL {
		err = m.attemptInplaceDDL(ctx)
		if err == nil {
			m.usedInplaceDDL = true // success
			return nil
		}
	}
	// Failure is expected, since MySQL DDL only applies in limited scenarios
	// Return the error, which will be ignored by the caller.
	// Proceed with regular copy algorithm.
	return err
}

func (m *Runner) dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", m.username, m.password, m.host, m.schemaName)
}

func (m *Runner) setup(ctx context.Context) error {
	// Drop the old table. It shouldn't exist, but it could.
	if err := m.dropOldTable(ctx); err != nil {
		return err
	}

	// First attempt to resume from a checkpoint.
	// It's OK if it fails, it just means it's a fresh migration.
	if err := m.resumeFromCheckpoint(ctx); err != nil {
		// Resume failed, do the initial steps.
		m.logger.Info("could not resume from checkpoint")
		if err := m.createNewTable(ctx); err != nil {
			return err
		}
		if err := m.alterNewTable(ctx); err != nil {
			return err
		}
		if err := m.createCheckpointTable(ctx); err != nil {
			return err
		}
		m.copier, err = row.NewCopier(m.db, m.table, m.newTable, &row.CopierConfig{
			Concurrency:           m.optConcurrency,
			TargetChunkTime:       m.optTargetChunkTime,
			FinalChecksum:         m.optChecksum,
			DisableTrivialChunker: m.optDisableTrivialChunker,
			Throttler:             &throttler.Noop{},
			Logger:                m.logger,
		})
		if err != nil {
			return err
		}
		m.replClient = repl.NewClient(m.db, m.host, m.table, m.newTable, m.username, m.password, &repl.ClientConfig{
			Logger:      m.logger,
			Concurrency: m.optConcurrency,
			BatchSize:   repl.DefaultBatchSize,
		})
		// Start the binary log feed now
		if err := m.replClient.Run(); err != nil {
			return err
		}
	}

	// If the replica DSN was specified, attach a replication throttler.
	// Otherwise it will default to the NOOP throttler.
	var err error
	if m.optReplicaDSN != "" {
		m.replica, err = sql.Open("mysql", m.optReplicaDSN)
		if err != nil {
			return err
		}
		// An error here means the connection to the replica is not valid, or it can't be detected
		// This is fatal because if a user specifies a replica throttler and it can't be used,
		// we should not proceed.
		m.throttler, err = throttler.NewReplicationThrottler(m.replica, m.optReplicaMaxLag, m.logger)
		if err != nil {
			m.logger.Warnf("could not create replication throttler: %v", err)
			return err
		}
		m.copier.SetThrottler(m.throttler)
		if err := m.throttler.Open(); err != nil {
			return err
		}
	}

	// Make sure the definition of the table never changes.
	// If it does, we could be in trouble.
	m.replClient.TableChangeNotificationCallback = m.tableChangeNotification
	// Make sure the replClient has a way to know where the copier is at.
	// If this is NOT nil then it will use this optimization when determining
	// if it can ignore a KEY.
	m.replClient.KeyAboveCopierCallback = m.copier.KeyAboveHighWatermark
	return nil
}

func (m *Runner) tableChangeNotification() {
	// It's an async message, so we don't know the current state
	// from which this "notification" was generated, but typically if our
	// current state is now in cutover, we can ignore it.
	if m.getCurrentState() >= stateCutOver {
		return
	}
	m.setCurrentState(stateErrCleanup)
	// Write this to the logger, so it can be captured by the initiator.
	m.logger.Error("table definition changed during migration")
	// Invalidate the checkpoint, so we don't try to resume.
	// If we don't do this, the migration will permanently be blocked from proceeding.
	// Letting it start again is the better choice.
	if err := m.dropCheckpoint(context.Background()); err != nil {
		m.logger.Errorf("could not remove checkpoint. err: %v", err)
	}
	// We can't do anything about it, just panic
	panic("table definition changed during migration")
}

func (m *Runner) dropCheckpoint(ctx context.Context) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", m.checkpointTable.QuotedName())
	_, err := m.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	return nil
}

func (m *Runner) createNewTable(ctx context.Context) error {
	newName := fmt.Sprintf("_%s_new", m.table.TableName)
	if len(newName) > 64 {
		return fmt.Errorf("table name is too long: '%s'. new table name will exceed 64 characters", m.table.TableName)
	}
	// drop both if we've decided to call this func.
	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", m.table.SchemaName, newName)
	_, err := m.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	query = fmt.Sprintf("CREATE TABLE `%s`.`%s` LIKE %s", m.table.SchemaName, newName, m.table.QuotedName())
	_, err = m.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	m.newTable = table.NewTableInfo(m.db, m.schemaName, newName)
	if err := m.newTable.SetInfo(ctx); err != nil {
		return err
	}
	return nil
}

// alterNewTable applies the ALTER to the new table.
// It has been pre-checked it is not a rename, or modifying the PRIMARY KEY.
func (m *Runner) alterNewTable(ctx context.Context) error {
	query := fmt.Sprintf("ALTER TABLE %s %s", m.newTable.QuotedName(), m.alterStatement)
	_, err := m.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	// Call GetInfo on the table again, since the columns
	// might have changed and this will affect the row copier's intersect func.
	return m.newTable.SetInfo(ctx)
}

func (m *Runner) dropOldTable(ctx context.Context) error {
	oldName := fmt.Sprintf("_%s_old", m.table.TableName)
	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", m.table.SchemaName, oldName)
	_, err := m.db.ExecContext(ctx, query)
	return err
}

func (m *Runner) attemptInstantDDL(ctx context.Context) error {
	query := fmt.Sprintf("ALTER TABLE %s %s, ALGORITHM=INSTANT", m.table.QuotedName(), m.alterStatement)
	_, err := m.db.ExecContext(ctx, query)
	return err
}

func (m *Runner) attemptInplaceDDL(ctx context.Context) error {
	query := fmt.Sprintf("ALTER TABLE %s %s, ALGORITHM=INPLACE, LOCK=NONE", m.table.QuotedName(), m.alterStatement)
	_, err := m.db.ExecContext(ctx, query)
	return err
}

func (m *Runner) createCheckpointTable(ctx context.Context) error {
	cpName := fmt.Sprintf("_%s_chkpnt", m.table.TableName)
	// drop both if we've decided to call this func.
	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", m.table.SchemaName, cpName)
	_, err := m.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	query = fmt.Sprintf("CREATE TABLE `%s`.`%s` (id int NOT NULL AUTO_INCREMENT PRIMARY KEY, copy_rows_at TEXT, binlog_name VARCHAR(255), binlog_pos INT, copy_rows BIGINT, alter_statement TEXT)",
		m.table.SchemaName, cpName)
	_, err = m.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	m.checkpointTable = table.NewTableInfo(m.db, m.table.SchemaName, cpName)
	if err != nil {
		return err
	}
	return nil
}

func (m *Runner) Close() error {
	m.setCurrentState(stateClose)
	if m.newTable == nil {
		return nil
	}
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", m.newTable.QuotedName())
	_, err := m.db.Exec(query)
	if err != nil {
		return err
	}

	if m.checkpointTable == nil {
		return nil
	}

	query = fmt.Sprintf("DROP TABLE IF EXISTS %s", m.checkpointTable.QuotedName())
	_, err = m.db.Exec(query)
	if err != nil {
		return err
	}
	if m.replClient != nil {
		m.replClient.Close()
	}
	if m.throttler != nil {
		m.throttler.Close()
	}
	if m.replica != nil {
		m.replica.Close()
	}
	return m.db.Close()
}

func (m *Runner) resumeFromCheckpoint(ctx context.Context) error {
	// Check that the new table exists and the checkpoint table
	// has at least one row in it.

	// The objects for these are not available until we confirm
	// tables exist and we
	newName := fmt.Sprintf("_%s_new", m.table.TableName)
	cpName := fmt.Sprintf("_%s_chkpnt", m.table.TableName)

	// Make sure we can read from the new table.
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT 1",
		m.schemaName, newName)
	_, err := m.db.Exec(query)
	if err != nil {
		return err
	}

	query = fmt.Sprintf("SELECT copy_rows_at, binlog_name, binlog_pos, copy_rows, alter_statement FROM `%s`.`%s` ORDER BY id DESC LIMIT 1",
		m.schemaName, cpName)
	var copyRowsAt, binlogName, alterStatement string
	var binlogPos int
	var copyRows int64
	err = m.db.QueryRow(query).Scan(&copyRowsAt, &binlogName, &binlogPos, &copyRows, &alterStatement)
	if err != nil {
		return err
	}
	if m.alterStatement != alterStatement {
		return errors.New("alter statement in checkpoint table does not match the alter statement specified here")
	}
	// Populate the objects that would have been set in the other funcs.
	m.newTable = table.NewTableInfo(m.db, m.schemaName, newName)
	if err := m.newTable.SetInfo(ctx); err != nil {
		return err
	}

	// In resume-from-checkpoint we need to ignore duplicate key errors when
	// applying copy-rows because we will partially re-apply some rows.
	// The problem with this is, we can't tell if it's not a re-apply but a new
	// row that's a duplicate and violating a new UNIQUE constraint we are trying
	// to add. The only way we can reconcile this fact is to make sure that
	// we checksum the table at the end. Thus, resume-from-checkpoint MUST
	// have the checksum enabled to apply all changes safely.
	m.optChecksum = true
	m.copier, err = row.NewCopierFromCheckpoint(m.db, m.table, m.newTable, &row.CopierConfig{
		Concurrency:           m.optConcurrency,
		TargetChunkTime:       m.optTargetChunkTime,
		FinalChecksum:         m.optChecksum,
		DisableTrivialChunker: m.optDisableTrivialChunker,
		Throttler:             &throttler.Noop{},
		Logger:                m.logger,
	}, copyRowsAt, copyRows)
	if err != nil {
		return err
	}

	// Set the binlog position.
	// Create a binlog subscriber
	m.replClient = repl.NewClient(m.db, m.host, m.table, m.newTable, m.username, m.password, &repl.ClientConfig{
		Logger:      m.logger,
		Concurrency: m.optConcurrency,
		BatchSize:   repl.DefaultBatchSize,
	})
	m.replClient.SetPos(&mysql.Position{
		Name: binlogName,
		Pos:  uint32(binlogPos),
	})

	m.checkpointTable = table.NewTableInfo(m.db, m.table.SchemaName, cpName)
	if err != nil {
		return err
	}

	// Start the replClient now. This is because if the checkpoint is so old there
	// are no longer binary log files, we want to abandon resume-from-checkpoint
	// and still be able to start from scratch.
	// Start the binary log feed just before copy rows starts.
	if err := m.replClient.Run(); err != nil {
		m.logger.Warnf("resuming from checkpoint failed because resuming from the previous binlog position failed. log-file: %s log-pos: %d", binlogName, binlogPos)
		return err
	}
	m.logger.Warnf("resuming from checkpoint. low-watermark: %s log-file: %s log-pos: %d copy-rows: %d", copyRowsAt, binlogName, binlogPos, copyRows)
	return nil
}

// checksum creates the checksum which opens the read view.
func (m *Runner) checksum(ctx context.Context) error {
	m.setCurrentState(stateChecksum)
	var err error
	m.checker, err = checksum.NewChecker(m.db, m.table, m.newTable, m.replClient, &checksum.CheckerConfig{
		Concurrency:           m.optChecksumConcurrency,
		TargetChunkTime:       m.optTargetChunkTime,
		DisableTrivialChunker: m.optDisableTrivialChunker,
		Logger:                m.logger,
	})
	if err != nil {
		return err
	}
	if err := m.checker.Run(ctx); err != nil {
		// Panic: this is really not expected to happen, and if it does
		// we don't want cleanup to happen in Close() so we can inspect it.
		panic(err)
	}
	m.logger.Info("checksum passed")

	// A long checksum extends the binlog deltas
	// So if we've called this optional checksum, we need one more state
	// of applying the binlog deltas.

	m.setCurrentState(statePostChecksum)
	m.periodicFlushLock.Lock() // Wait for the periodic flush to finish.
	if err := m.replClient.FlushUntilTrivial(ctx); err != nil {
		m.periodicFlushLock.Unlock() // need to yield before returning.
		return err
	}
	m.periodicFlushLock.Unlock() // It will not start again because the current state is now ApplyChangeset.

	return nil
}

func (m *Runner) getCurrentState() migrationState {
	return migrationState(atomic.LoadInt32((*int32)(&m.currentState)))
}

func (m *Runner) setCurrentState(s migrationState) {
	atomic.StoreInt32((*int32)(&m.currentState), int32(s))
	if s > stateCopyRows && m.replClient != nil {
		m.replClient.SetKeyAboveWatermarkOptimization(false)
	}
}

func (m *Runner) dumpCheckpoint(ctx context.Context) error {
	// Retrieve the binlog position first and under a mutex.
	// Currently it never advances but it's possible it might in future
	// and this race condition is missed.
	binlog := m.replClient.GetBinlogApplyPosition()
	lowWatermark, err := m.copier.GetLowWatermark()
	if err != nil {
		return err // it might not be ready, we can try again.
	}
	copyRows := atomic.LoadInt64(&m.copier.CopyRowsCount)
	m.logger.Infof("checkpoint: low-watermark=%s log-file=%s log-pos=%d copy-rows=%d", lowWatermark, binlog.Name, binlog.Pos, copyRows)
	query := fmt.Sprintf("INSERT INTO %s (copy_rows_at, binlog_name, binlog_pos, copy_rows, alter_statement) VALUES (?, ?, ?, ?, ?)",
		m.checkpointTable.QuotedName())
	_, err = m.db.ExecContext(ctx, query, lowWatermark, binlog.Name, binlog.Pos, copyRows, m.alterStatement)
	return err
}

func (m *Runner) dumpCheckpointContinuously(ctx context.Context) {
	ticker := time.NewTicker(checkpointDumpInterval)
	defer ticker.Stop()
	for range ticker.C {
		// Continue to checkpoint until we exit copy-rows.
		// Ideally in future we can continue further than this,
		// but unfortunately this currently results in a
		// "watermark not ready" error.
		if m.getCurrentState() > stateCopyRows {
			return
		}
		if err := m.dumpCheckpoint(ctx); err != nil {
			m.logger.Errorf("error writing checkpoint: %v", err)
		}
	}
}

func (m *Runner) periodicFlush(ctx context.Context) {
	ticker := time.NewTicker(binlogPerodicFlushInterval)
	defer ticker.Stop()
	for range ticker.C {
		// We only want to continuously flush during copy-rows.
		// During checkpoint we only lock the source table, so if we
		// are in a periodic flush we can't be writing data to the new table.
		// If we do, then we'll need to lock it as well so that the read-view is consistent.
		m.periodicFlushLock.Lock()
		if m.getCurrentState() > stateCopyRows {
			m.periodicFlushLock.Unlock()
			return
		}
		startLoop := time.Now()
		m.logger.Info("starting periodic flush of binary log")
		// The periodic flush does not respect the throttler. It is only single-threaded
		// by design (chunked into 10K rows). Since we want to advance the binlog position
		// we allow this to run, and then expect that if it is under load the throttler
		// will kick in and slow down the copy-rows.
		if err := m.replClient.Flush(ctx); err != nil {
			m.logger.Errorf("error flushing binary log: %v", err)
		}
		m.periodicFlushLock.Unlock()
		m.logger.Infof("finished periodic flush of binary log: duration=%v", time.Since(startLoop))
	}
}

func (m *Runner) updateTableStatisticsContinuously(ctx context.Context) {
	ticker := time.NewTicker(tableStatUpdateInterval)
	defer ticker.Stop()
	for range ticker.C {
		// We need to update statistics for copy-rows and checksum
		if m.getCurrentState() > stateCutOver {
			return
		}
		if err := m.table.UpdateTableStatistics(ctx); err != nil {
			m.logger.Errorf("error updating table statistics: %v", err)
		}
		m.logger.Infof("table statistics updated: estimated-rows=%d pk[0].max-value=%v", m.table.EstimatedRows, m.table.MaxValue())
	}
}

func (m *Runner) dumpStatus() {
	ticker := time.NewTicker(statusInterval)
	defer ticker.Stop()
	for range ticker.C {
		state := m.getCurrentState()
		if state > stateCutOver {
			return
		}

		switch state {
		case stateCopyRows:
			// Status for copy rows
			pct := float64(atomic.LoadInt64(&m.copier.CopyRowsCount)) / float64(m.table.EstimatedRows) * 100
			m.logger.Infof("migration status: state=%s copy-progress=%s binlog-deltas=%v total-time=%s copier-time=%s copier-remaining-time=%v copier-is-throttled=%v",
				m.getCurrentState().String(),
				fmt.Sprintf("%.2f%%", pct),
				m.replClient.GetDeltaLen(),
				time.Since(m.startTime),
				time.Since(m.copier.StartTime),
				m.getETAFromRowsPerSecond(pct > 99.9),
				m.copier.Throttler.IsThrottled(),
			)
		case stateApplyChangeset, statePostChecksum:
			// We've finished copying rows and we are now trying to reduce the number of binlog deltas before
			// proceeding to the checksum and then the final cutover.
			m.logger.Infof("migration status: state=%s binlog-deltas=%v time-total=%v",
				m.getCurrentState().String(),
				m.replClient.GetDeltaLen(),
				time.Since(m.startTime).String(),
			)
		case stateChecksum:
			// This could take a while if it's a large table. We just have to show approximate progress.
			// This is a little bit harder for checksum because it doesn't have returned rows
			// so we just show a "recent value" over the "maximum value".
			m.logger.Infof("migration status: state=%s checksum-progress=%s/%s binlog-deltas=%v total-total=%s checksum-time=%s",
				m.getCurrentState().String(),
				m.checker.RecentValue(), m.table.MaxValue(),
				m.replClient.GetDeltaLen(),
				time.Since(m.startTime),
				time.Since(m.checker.StartTime),
			)
		default:
			// For the linter:
			// Status for all other states
		}
	}
}

func (m *Runner) estimateRowsPerSecondLoop() {
	// We take 10 second averages not 1 second
	// because with parallel copy it bounces around a lot.
	prevRowsCount := atomic.LoadInt64(&m.copier.CopyRowsCount)
	ticker := time.NewTicker(copyEstimateInterval)
	defer ticker.Stop()
	for range ticker.C {
		if m.getCurrentState() > stateCopyRows {
			return
		}
		newRowsCount := atomic.LoadInt64(&m.copier.CopyRowsCount)
		rowsPerSecond := (newRowsCount - prevRowsCount) / 10
		atomic.StoreInt64(&m.copier.EtaRowsPerSecond, rowsPerSecond)
		prevRowsCount = newRowsCount
	}
}

func (m *Runner) getETAFromRowsPerSecond(due bool) string {
	rowsPerSecond := atomic.LoadInt64(&m.copier.EtaRowsPerSecond)
	if m.getCurrentState() > stateCopyRows || due {
		return "Due" // in apply rows phase or checksum
	}
	if rowsPerSecond == 0 {
		return "TBD" // not enough data yet, or in last phase
	}

	remainingRows := m.table.EstimatedRows - uint64(atomic.LoadInt64(&m.copier.CopyRowsCount))
	remainingSeconds := math.Floor(float64(remainingRows) / float64(rowsPerSecond))

	// We could just return it as "12345 seconds" but to group it to hours/days.
	// We convert to time.Duration which will interpret this as nanoseconds,
	// so we need to multiply by seconds.
	return time.Duration(remainingSeconds * float64(time.Second)).String()
}

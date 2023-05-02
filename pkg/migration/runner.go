package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/squareup/spirit/pkg/metrics"

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

// These are really consts, but set to var for testing.
var (
	checkpointDumpInterval  = 50 * time.Second
	tableStatUpdateInterval = 5 * time.Minute
	statusInterval          = 10 * time.Second
	// binlogPerodicFlushInterval is the time that the client will flush all binlog changes to disk.
	// Longer values require more memory, but permit more merging.
	// I expect we will change this to 1hr-24hr in the future.
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
	migration       *Migration
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

	// Attached logger
	logger loggers.Advanced

	// MetricsSink
	metricsSink metrics.Sink
}

func NewRunner(m *Migration) (*Runner, error) {
	r := &Runner{
		migration:   m,
		logger:      logrus.New(),
		metricsSink: metrics.NewNoopSink(),
	}
	if r.migration.TargetChunkTime == 0 {
		r.migration.TargetChunkTime = 100 * time.Millisecond
	}
	if r.migration.Threads == 0 {
		r.migration.Threads = 4
	}
	if r.migration.ReplicaMaxLag == 0 {
		r.migration.ReplicaMaxLag = 120 * time.Second
	}
	if r.migration.Host == "" {
		return nil, errors.New("host is required")
	}
	if !strings.Contains(r.migration.Host, ":") {
		r.migration.Host = fmt.Sprintf("%s:%d", r.migration.Host, 3306)
	}
	if r.migration.Database == "" {
		return nil, errors.New("schema name is required")
	}
	if r.migration.Table == "" {
		return nil, errors.New("table name is required")
	}
	if r.migration.Alter == "" {
		return nil, errors.New("alter statement is required")
	}
	return r, nil
}

func (r *Runner) SetMetricsSink(sink metrics.Sink) {
	r.metricsSink = sink
}

func (r *Runner) SetLogger(logger loggers.Advanced) {
	r.logger = logger
}

func (r *Runner) Run(ctx context.Context) error {
	r.startTime = time.Now()
	r.logger.Infof("Starting spirit migration: concurrency=%d target-chunk-size=%s table=%s.%s alter=\"%s\"",
		r.migration.Threads, r.migration.TargetChunkTime, r.migration.Database, r.migration.Table, r.migration.Alter,
	)

	// Create a database connection
	// It will be closed in r.Close()
	var err error
	r.db, err = sql.Open("mysql", r.dsn())
	if err != nil {
		return err
	}
	if err := r.db.Ping(); err != nil {
		return err
	}

	// Get Table Info
	r.table = table.NewTableInfo(r.db, r.migration.Database, r.migration.Table)
	if err := r.table.SetInfo(ctx); err != nil {
		return err
	}
	// This step is technically optional, but first we attempt to
	// use MySQL's built-in DDL. This is because it's usually faster
	// when it is compatible. If it returns no error, that means it
	// has been successful and the DDL is complete.
	err = r.attemptMySQLDDL(ctx)
	if err == nil {
		r.logger.Infof("apply complete: instant-ddl=%v inplace-ddl=%v", r.usedInstantDDL, r.usedInplaceDDL)
		return nil // success!
	}

	// Perform preflight basic checks.
	if err := r.runChecks(ctx, check.ScopePreflight); err != nil {
		return err
	}

	// Perform setup steps, including resuming from a checkpoint (if available)
	// and creating the new and checkpoint tables.
	// The replication client is also created here.
	if err := r.setup(ctx); err != nil {
		return err
	}

	// Run post-setup checks
	if err := r.runChecks(ctx, check.ScopePostSetup); err != nil {
		return err
	}

	go r.dumpStatus()                    // start periodically writing status
	go r.dumpCheckpointContinuously(ctx) // start periodically dumping the checkpoint.

	// Perform the main copy rows task. This is where the majority
	// of migrations usually spend time.
	r.setCurrentState(stateCopyRows)
	if err := r.copier.Run(ctx); err != nil {
		return err
	}
	r.logger.Info("copy rows complete")

	// Perform steps to prepare for final cutover.
	// This includes computing an optional checksum,
	// catching up on replClient apply, running ANALYZE TABLE so
	// that the statistics will be up-to-date on cutover.
	if err := r.prepareForCutover(ctx); err != nil {
		return err
	}
	// Run any checks that need to be done pre-cutover.
	if err := r.runChecks(ctx, check.ScopeCutover); err != nil {
		return err
	}
	// It's time for the final cut-over, where
	// the tables are swapped under a lock.
	r.setCurrentState(stateCutOver)
	cutover, err := NewCutOver(r.db, r.table, r.newTable, r.replClient, r.logger)
	if err != nil {
		return err
	}
	// Drop the _old table if it exists. This ensures
	// that the rename will succeed (although there is a brief race)
	if err := r.dropOldTable(ctx); err != nil {
		return err
	}
	if err := cutover.Run(ctx); err != nil {
		return err
	}

	checksumTime := time.Duration(0)
	if r.checker != nil {
		checksumTime = r.checker.ExecTime
	}
	r.logger.Infof("apply complete: instant-ddl=%v inplace-ddl=%v total-chunks=%v copy-rows-time=%s checksum-time=%s total-time=%s",
		r.usedInstantDDL,
		r.usedInplaceDDL,
		r.copier.CopyChunksCount,
		r.copier.ExecTime,
		checksumTime,
		time.Since(r.startTime),
	)
	return nil
}

// prepareForCutover performs steps to prepare for the final cutover.
// most of these steps are technically optional, but skipping them
// could for example cause a stall during the cutover if the replClient
// has too many pending updates.
func (r *Runner) prepareForCutover(ctx context.Context) error {
	r.setCurrentState(stateApplyChangeset)
	// Disable the periodic flush and flush all pending events.
	r.replClient.StopPeriodicFlush()
	if err := r.replClient.Flush(ctx); err != nil {
		return err
	}

	// Run ANALYZE TABLE to update the statistics on the new table.
	// This is required so on cutover plans don't go sideways, which
	// is at elevated risk because the batch loading can cause statistics
	// to be out of date.
	r.setCurrentState(stateAnalyzeTable)
	stmt := fmt.Sprintf("ANALYZE TABLE %s", r.newTable.QuotedName)
	r.logger.Infof("Running: %s", stmt)
	if err := dbconn.DBExec(ctx, r.db, stmt); err != nil {
		return err
	}

	// The checksum is (usually) optional, but it is ONLINE after an initial lock
	// for consistency. It is the main way that we determine that
	// this program is safe to use even when immature. In the event that it is
	// a resume-from-checkpoint operation, the checksum is NOT optional.
	// This is because adding a unique index can not be differentiated from a
	// duplicate key error caused by retrying partial work.
	if r.migration.Checksum {
		if err := r.checksum(ctx); err != nil {
			return err
		}
	}
	return nil
}

// runChecks wraps around check.RunChecks and adds the context of this migration
func (r *Runner) runChecks(ctx context.Context, scope check.ScopeFlag) error {
	return check.RunChecks(ctx, check.Resources{
		DB:              r.db,
		Replica:         r.replica,
		Table:           r.table,
		Alter:           r.migration.Alter,
		TargetChunkTime: r.migration.TargetChunkTime,
		Threads:         r.migration.Threads,
		ReplicaMaxLag:   r.migration.ReplicaMaxLag,
	}, r.logger, scope)
}

// attemptMySQLDDL "attempts" to use DDL directly on MySQL with an assertion
// such as ALGORITHM=INSTANT. If MySQL is able to use the INSTANT algorithm,
// it will perform the operation without error. If it can't, it will return
// an error. It is important to let MySQL decide if it can handle the DDL
// operation, because keeping track of which operations are "INSTANT"
// is incredibly difficult. It will depend on MySQL minor version,
// and could possibly be specific to the table.
func (r *Runner) attemptMySQLDDL(ctx context.Context) error {
	err := r.attemptInstantDDL(ctx)
	if err == nil {
		r.usedInstantDDL = true // success
		return nil
	}
	// Inplace DDL is feature gated because it blocks replicas.
	// It's only safe to do in aurora GLOBAL because replicas do not
	// use the binlog.
	if r.migration.AttemptInplaceDDL {
		err = r.attemptInplaceDDL(ctx)
		if err == nil {
			r.usedInplaceDDL = true // success
			return nil
		}
	}
	// Failure is expected, since MySQL DDL only applies in limited scenarios
	// Return the error, which will be ignored by the caller.
	// Proceed with regular copy algorithm.
	return err
}

func (r *Runner) dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", r.migration.Username, r.migration.Password, r.migration.Host, r.migration.Database)
}

func (r *Runner) setup(ctx context.Context) error {
	// Drop the old table. It shouldn't exist, but it could.
	if err := r.dropOldTable(ctx); err != nil {
		return err
	}

	// First attempt to resume from a checkpoint.
	// It's OK if it fails, it just means it's a fresh migration.
	if err := r.resumeFromCheckpoint(ctx); err != nil {
		// Resume failed, do the initial steps.
		r.logger.Infof("could not resume from checkpoint: reason=%s", err)
		if err := r.createNewTable(ctx); err != nil {
			return err
		}
		if err := r.alterNewTable(ctx); err != nil {
			return err
		}
		if err := r.createCheckpointTable(ctx); err != nil {
			return err
		}
		r.copier, err = row.NewCopier(r.db, r.table, r.newTable, &row.CopierConfig{
			Concurrency:     r.migration.Threads,
			TargetChunkTime: r.migration.TargetChunkTime,
			FinalChecksum:   r.migration.Checksum,
			Throttler:       &throttler.Noop{},
			Logger:          r.logger,
		})
		if err != nil {
			return err
		}
		r.replClient = repl.NewClient(r.db, r.migration.Host, r.table, r.newTable, r.migration.Username, r.migration.Password, &repl.ClientConfig{
			Logger:      r.logger,
			Concurrency: r.migration.Threads,
			BatchSize:   repl.DefaultBatchSize,
		})
		// Start the binary log feed now
		if err := r.replClient.Run(); err != nil {
			return err
		}
	}

	// If the replica DSN was specified, attach a replication throttler.
	// Otherwise, it will default to the NOOP throttler.
	var err error
	if r.migration.ReplicaDSN != "" {
		r.replica, err = sql.Open("mysql", r.migration.ReplicaDSN)
		if err != nil {
			return err
		}
		// An error here means the connection to the replica is not valid, or it can't be detected
		// This is fatal because if a user specifies a replica throttler, and it can't be used,
		// we should not proceed.
		r.throttler, err = throttler.NewReplicationThrottler(r.replica, r.migration.ReplicaMaxLag, r.logger)
		if err != nil {
			r.logger.Warnf("could not create replication throttler: %v", err)
			return err
		}
		r.copier.SetThrottler(r.throttler)
		if err := r.throttler.Open(); err != nil {
			return err
		}
	}

	// Make sure the definition of the table never changes.
	// If it does, we could be in trouble.
	r.replClient.TableChangeNotificationCallback = r.tableChangeNotification
	// Make sure the replClient has a way to know where the copier is at.
	// If this is NOT nil then it will use this optimization when determining
	// if it can ignore a KEY.
	r.replClient.KeyAboveCopierCallback = r.copier.KeyAboveHighWatermark

	// Start routines in table and replication packages to
	// Continuously update the min/max and estimated rows
	// and to flush the binary log position periodically.
	go r.table.AutoUpdateStatistics(ctx, tableStatUpdateInterval, r.logger)
	go r.replClient.StartPeriodicFlush(ctx, binlogPerodicFlushInterval)
	return nil
}

func (r *Runner) tableChangeNotification() {
	// It's an async message, so we don't know the current state
	// from which this "notification" was generated, but typically if our
	// current state is now in cutover, we can ignore it.
	if r.getCurrentState() >= stateCutOver {
		return
	}
	r.setCurrentState(stateErrCleanup)
	// Write this to the logger, so it can be captured by the initiator.
	r.logger.Error("table definition changed during migration")
	// Invalidate the checkpoint, so we don't try to resume.
	// If we don't do this, the migration will permanently be blocked from proceeding.
	// Letting it start again is the better choice.
	if err := r.dropCheckpoint(context.Background()); err != nil {
		r.logger.Errorf("could not remove checkpoint. err: %v", err)
	}
	// We can't do anything about it, just panic
	panic("table definition changed during migration")
}

func (r *Runner) dropCheckpoint(ctx context.Context) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", r.checkpointTable.QuotedName)
	_, err := r.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	return nil
}

func (r *Runner) createNewTable(ctx context.Context) error {
	newName := fmt.Sprintf("_%s_new", r.table.TableName)
	if len(newName) > 64 {
		return fmt.Errorf("table name is too long: '%s'. new table name will exceed 64 characters", r.table.TableName)
	}
	// drop both if we've decided to call this func.
	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", r.table.SchemaName, newName)
	_, err := r.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	query = fmt.Sprintf("CREATE TABLE `%s`.`%s` LIKE %s", r.table.SchemaName, newName, r.table.QuotedName)
	_, err = r.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	r.newTable = table.NewTableInfo(r.db, r.migration.Database, newName)
	if err := r.newTable.SetInfo(ctx); err != nil {
		return err
	}
	return nil
}

// alterNewTable applies the ALTER to the new table.
// It has been pre-checked it is not a rename, or modifying the PRIMARY KEY.
func (r *Runner) alterNewTable(ctx context.Context) error {
	query := fmt.Sprintf("ALTER TABLE %s %s", r.newTable.QuotedName, r.migration.Alter)
	_, err := r.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	// Call GetInfo on the table again, since the columns
	// might have changed and this will affect the row copiers intersect func.
	return r.newTable.SetInfo(ctx)
}

func (r *Runner) dropOldTable(ctx context.Context) error {
	oldName := fmt.Sprintf("_%s_old", r.table.TableName)
	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", r.table.SchemaName, oldName)
	_, err := r.db.ExecContext(ctx, query)
	return err
}

func (r *Runner) attemptInstantDDL(ctx context.Context) error {
	query := fmt.Sprintf("ALTER TABLE %s %s, ALGORITHM=INSTANT", r.table.QuotedName, r.migration.Alter)
	_, err := r.db.ExecContext(ctx, query)
	return err
}

func (r *Runner) attemptInplaceDDL(ctx context.Context) error {
	query := fmt.Sprintf("ALTER TABLE %s %s, ALGORITHM=INPLACE, LOCK=NONE", r.table.QuotedName, r.migration.Alter)
	_, err := r.db.ExecContext(ctx, query)
	return err
}

func (r *Runner) createCheckpointTable(ctx context.Context) error {
	cpName := fmt.Sprintf("_%s_chkpnt", r.table.TableName)
	// drop both if we've decided to call this func.
	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", r.table.SchemaName, cpName)
	_, err := r.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	query = fmt.Sprintf("CREATE TABLE `%s`.`%s` (id int NOT NULL AUTO_INCREMENT PRIMARY KEY, low_watermark TEXT,  binlog_name VARCHAR(255), binlog_pos INT, rows_copied BIGINT, rows_copied_logical BIGINT, alter_statement TEXT)",
		r.table.SchemaName, cpName)
	_, err = r.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	r.checkpointTable = table.NewTableInfo(r.db, r.table.SchemaName, cpName)
	if err != nil {
		return err
	}
	return nil
}

func (r *Runner) Close() error {
	r.setCurrentState(stateClose)
	if r.table != nil {
		err := r.table.Close()
		if err != nil {
			return err
		}
	}
	if r.newTable != nil {
		query := fmt.Sprintf("DROP TABLE IF EXISTS %s", r.newTable.QuotedName)
		_, err := r.db.Exec(query)
		if err != nil {
			return err
		}
	}

	if r.checkpointTable != nil {
		err := r.dropCheckpoint(context.TODO())
		if err != nil {
			return err
		}
	}

	if r.replClient != nil {
		r.replClient.Close()
	}
	if r.throttler != nil {
		err := r.throttler.Close()
		if err != nil {
			return err
		}
	}
	if r.replica != nil {
		err := r.replica.Close()
		if err != nil {
			return err
		}
	}
	return r.db.Close()
}

func (r *Runner) resumeFromCheckpoint(ctx context.Context) error {
	// Check that the new table exists and the checkpoint table
	// has at least one row in it.

	// The objects for these are not available until we confirm
	// tables exist and we
	newName := fmt.Sprintf("_%s_new", r.table.TableName)
	cpName := fmt.Sprintf("_%s_chkpnt", r.table.TableName)

	// Make sure we can read from the new table.
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT 1",
		r.migration.Database, newName)
	_, err := r.db.Exec(query)
	if err != nil {
		return fmt.Errorf("could not read from table '%s'", newName)
	}

	query = fmt.Sprintf("SELECT low_watermark, binlog_name, binlog_pos, rows_copied, rows_copied_logical, alter_statement FROM `%s`.`%s` ORDER BY id DESC LIMIT 1",
		r.migration.Database, cpName)
	var lowWatermark, binlogName, alterStatement string
	var binlogPos int
	var rowsCopied, rowsCopiedLogical uint64
	err = r.db.QueryRow(query).Scan(&lowWatermark, &binlogName, &binlogPos, &rowsCopied, &rowsCopiedLogical, &alterStatement)
	if err != nil {
		return fmt.Errorf("could not read from table '%s'", cpName)
	}
	if r.migration.Alter != alterStatement {
		return errors.New("alter statement in checkpoint table does not match the alter statement specified here")
	}
	// Populate the objects that would have been set in the other funcs.
	r.newTable = table.NewTableInfo(r.db, r.migration.Database, newName)
	if err := r.newTable.SetInfo(ctx); err != nil {
		return err
	}

	// In resume-from-checkpoint we need to ignore duplicate key errors when
	// applying copy-rows because we will partially re-apply some rows.
	// The problem with this is, we can't tell if it's not a re-apply but a new
	// row that's a duplicate and violating a new UNIQUE constraint we are trying
	// to add. The only way we can reconcile this fact is to make sure that
	// we checksum the table at the end. Thus, resume-from-checkpoint MUST
	// have the checksum enabled to apply all changes safely.
	r.migration.Checksum = true
	r.copier, err = row.NewCopierFromCheckpoint(r.db, r.table, r.newTable, &row.CopierConfig{
		Concurrency:     r.migration.Threads,
		TargetChunkTime: r.migration.TargetChunkTime,
		FinalChecksum:   r.migration.Checksum,
		Throttler:       &throttler.Noop{},
		Logger:          r.logger,
	}, lowWatermark, rowsCopied, rowsCopiedLogical)
	if err != nil {
		return err
	}

	// Set the binlog position.
	// Create a binlog subscriber
	r.replClient = repl.NewClient(r.db, r.migration.Host, r.table, r.newTable, r.migration.Username, r.migration.Password, &repl.ClientConfig{
		Logger:      r.logger,
		Concurrency: r.migration.Threads,
		BatchSize:   repl.DefaultBatchSize,
	})
	r.replClient.SetPos(&mysql.Position{
		Name: binlogName,
		Pos:  uint32(binlogPos),
	})

	r.checkpointTable = table.NewTableInfo(r.db, r.table.SchemaName, cpName)
	if err != nil {
		return err
	}

	// Start the replClient now. This is because if the checkpoint is so old there
	// are no longer binary log files, we want to abandon resume-from-checkpoint
	// and still be able to start from scratch.
	// Start the binary log feed just before copy rows starts.
	if err := r.replClient.Run(); err != nil {
		r.logger.Warnf("resuming from checkpoint failed because resuming from the previous binlog position failed. log-file: %s log-pos: %d", binlogName, binlogPos)
		return err
	}
	r.logger.Warnf("resuming from checkpoint. low-watermark: %s log-file: %s log-pos: %d copy-rows: %d", lowWatermark, binlogName, binlogPos, rowsCopied)
	return nil
}

// checksum creates the checksum which opens the read view.
func (r *Runner) checksum(ctx context.Context) error {
	r.setCurrentState(stateChecksum)
	var err error
	r.checker, err = checksum.NewChecker(r.db, r.table, r.newTable, r.replClient, &checksum.CheckerConfig{
		Concurrency:     r.migration.Threads,
		TargetChunkTime: r.migration.TargetChunkTime,
		Logger:          r.logger,
	})
	if err != nil {
		return err
	}
	if err := r.checker.Run(ctx); err != nil {
		// Panic: this is really not expected to happen, and if it does
		// we don't want cleanup to happen in Close() so we can inspect it.
		panic(err)
	}
	r.logger.Info("checksum passed")

	// A long checksum extends the binlog deltas
	// So if we've called this optional checksum, we need one more state
	// of applying the binlog deltas.

	r.setCurrentState(statePostChecksum)
	return r.replClient.Flush(ctx)
}

func (r *Runner) getCurrentState() migrationState {
	return migrationState(atomic.LoadInt32((*int32)(&r.currentState)))
}

func (r *Runner) setCurrentState(s migrationState) {
	atomic.StoreInt32((*int32)(&r.currentState), int32(s))
	if s > stateCopyRows && r.replClient != nil {
		r.replClient.SetKeyAboveWatermarkOptimization(false)
	}
}

func (r *Runner) dumpCheckpoint(ctx context.Context) error {
	// Retrieve the binlog position first and under a mutex.
	// Currently, it never advances, but it's possible it might in future
	// and this race condition is missed.
	binlog := r.replClient.GetBinlogApplyPosition()
	lowWatermark, err := r.copier.GetLowWatermark()
	if err != nil {
		return err // it might not be ready, we can try again.
	}
	copyRows := atomic.LoadUint64(&r.copier.CopyRowsCount)
	logicalCopyRows := atomic.LoadUint64(&r.copier.CopyRowsLogicalCount)
	r.logger.Infof("checkpoint: low-watermark=%s log-file=%s log-pos=%d rows-copied=%d rows-copied-logical=%d", lowWatermark, binlog.Name, binlog.Pos, copyRows, logicalCopyRows)
	query := fmt.Sprintf("INSERT INTO %s (low_watermark, binlog_name, binlog_pos, rows_copied, rows_copied_logical, alter_statement) VALUES (?, ?, ?, ?, ?, ?)",
		r.checkpointTable.QuotedName)
	_, err = r.db.ExecContext(ctx, query, lowWatermark, binlog.Name, binlog.Pos, copyRows, logicalCopyRows, r.migration.Alter)
	return err
}

func (r *Runner) dumpCheckpointContinuously(ctx context.Context) {
	ticker := time.NewTicker(checkpointDumpInterval)
	defer ticker.Stop()
	for range ticker.C {
		// Continue to checkpoint until we exit copy-rows.
		// Ideally in future we can continue further than this,
		// but unfortunately this currently results in a
		// "watermark not ready" error.
		if r.getCurrentState() > stateCopyRows {
			return
		}
		if err := r.dumpCheckpoint(ctx); err != nil {
			r.logger.Errorf("error writing checkpoint: %v", err)
		}
	}
}

func (r *Runner) dumpStatus() {
	ticker := time.NewTicker(statusInterval)
	defer ticker.Stop()
	for range ticker.C {
		state := r.getCurrentState()
		if state > stateCutOver {
			return
		}

		switch state {
		case stateCopyRows:
			// Status for copy rows
			r.logger.Infof("migration status: state=%s copy-progress=%s binlog-deltas=%v total-time=%s copier-time=%s copier-remaining-time=%v copier-is-throttled=%v",
				r.getCurrentState().String(),
				r.copier.GetProgress(),
				r.replClient.GetDeltaLen(),
				time.Since(r.startTime),
				time.Since(r.copier.StartTime),
				r.copier.GetETA(),
				r.copier.Throttler.IsThrottled(),
			)
		case stateApplyChangeset, statePostChecksum:
			// We've finished copying rows, and we are now trying to reduce the number of binlog deltas before
			// proceeding to the checksum and then the final cutover.
			r.logger.Infof("migration status: state=%s binlog-deltas=%v time-total=%v",
				r.getCurrentState().String(),
				r.replClient.GetDeltaLen(),
				time.Since(r.startTime).String(),
			)
		case stateChecksum:
			// This could take a while if it's a large table. We just have to show approximate progress.
			// This is a little bit harder for checksum because it doesn't have returned rows
			// so we just show a "recent value" over the "maximum value".
			r.logger.Infof("migration status: state=%s checksum-progress=%s/%s binlog-deltas=%v total-total=%s checksum-time=%s",
				r.getCurrentState().String(),
				r.checker.RecentValue(), r.table.MaxValue(),
				r.replClient.GetDeltaLen(),
				time.Since(r.startTime),
				time.Since(r.checker.StartTime),
			)
		default:
			// For the linter:
			// Status for all other states
		}
	}
}

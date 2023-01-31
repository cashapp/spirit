package migration

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/squareup/gap-core/log"
	"github.com/squareup/spirit/pkg/checksum"
	"github.com/squareup/spirit/pkg/copier"
	"github.com/squareup/spirit/pkg/repl"
	"github.com/squareup/spirit/pkg/table"
	"github.com/squareup/spirit/pkg/throttler"
)

type migrationState int32

const (
	migrationStateInitial migrationState = iota
	migrationStateCopyRows
	migrationStateApplyChangeset
	migrationStateChecksum
	migrationStateCutOver
	migrationStateClose
	migrationStateErrCleanup
)

const (
	checkpointDumpInterval  = 5 * time.Second
	tableStatUpdateInterval = 5 * time.Minute
	statusInterval          = 2 * time.Second
	copyEstimateInterval    = 5 * time.Second
	// binlogPerodicFlushInterval is the time that the client will flush all binlog changes to disk.
	// Longer values require more memory, but permit more merging.
	// I expect we will change this to 1hr-24hr in future.
	binlogPerodicFlushInterval = 30 * time.Second
)

func (s migrationState) String() string {
	switch s {
	case migrationStateInitial:
		return "initial"
	case migrationStateCopyRows:
		return "copyRows"
	case migrationStateApplyChangeset:
		return "applyChangeset"
	case migrationStateChecksum:
		return "checksum"
	case migrationStateCutOver:
		return "cutOver"
	case migrationStateClose:
		return "close"
	case migrationStateErrCleanup:
		return "errCleanup"
	}
	return "unknown"
}

type MigrationRunner struct {
	host           string
	username       string
	password       string
	tableName      string
	schemaName     string
	alterStatement string

	db              *sql.DB
	table           *table.TableInfo
	shadowTable     *table.TableInfo
	checkpointTable *table.TableInfo

	currentState migrationState // must use atomic to get/set
	feed         *repl.Client   // feed contains all binlog subscription activity.
	copier       *copier.Copier

	// Track some key statistics.
	startTime time.Time

	// Used by the test-suite and some post-migration output.
	// Indicates if certain optimizations applied.
	usedInstantDDL bool
	usedInplaceDDL bool

	// Configurable Options that might be passed in
	// defaults will be set in NewMigrationRunner()
	optConcurrency           int
	optChecksumConcurrency   int
	optTargetChunkMs         int64
	optAttemptInplaceDDL     bool
	optChecksum              bool
	optDisableTrivialChunker bool
	optReplicaDSN            string
	optReplicaMaxLagMs       int64

	// We want to block periodic flushes while we're doing a checksums etc
	periodicFlushLock sync.Mutex

	// Attached logger
	logger *log.Logger
}

func NewMigrationRunner(migration *Migration) (*MigrationRunner, error) {
	// This might be replaced later.
	logger := log.New(log.LoggingConfig{
		ForceColor: true,
		Level:      "info",
	})
	m := &MigrationRunner{
		host:                     migration.Host,
		username:                 migration.Username,
		password:                 migration.Password,
		schemaName:               migration.Database,
		tableName:                migration.Table,
		startTime:                time.Now(),
		alterStatement:           migration.Alter,
		optConcurrency:           migration.Concurrency,
		optChecksumConcurrency:   migration.ChecksumConcurrency,
		optTargetChunkMs:         migration.TargetChunkMs,
		optAttemptInplaceDDL:     migration.AttemptInplaceDDL,
		optChecksum:              migration.Checksum,
		optDisableTrivialChunker: migration.DisableTrivialChunker,
		optReplicaDSN:            migration.ReplicaDSN,
		optReplicaMaxLagMs:       migration.ReplicaMaxLagMs,
		logger:                   logger,
	}
	if m.optTargetChunkMs == 0 {
		m.optTargetChunkMs = 100
	}
	if m.optConcurrency == 0 {
		m.optConcurrency = 4
	}
	if m.optChecksumConcurrency == 0 {
		m.optChecksumConcurrency = m.optConcurrency
	}
	if m.host == "" {
		return nil, fmt.Errorf("host is required")
	}
	if !strings.Contains(m.host, ":") {
		m.host = fmt.Sprintf("%s:%d", m.host, 3306)
	}
	if m.schemaName == "" {
		return nil, fmt.Errorf("schema name is required")
	}
	if m.tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}
	if m.alterStatement == "" {
		return nil, fmt.Errorf("alter statement is required")
	}
	return m, nil
}

func (m *MigrationRunner) SetLogger(logger *log.Logger) {
	m.logger = logger
}

func (m *MigrationRunner) Run(ctx context.Context) error {
	m.logger.Infof("Starting spirit migration")

	// Create a database connection
	// It will be closed in m.Close()
	var err error
	m.db, err = sql.Open("mysql", m.dsn())
	if err != nil {
		return err
	}

	// Get Table Info
	m.table = table.NewTableInfo(m.schemaName, m.tableName)
	if err := m.table.RunDiscovery(m.db); err != nil {
		return err
	}
	// Attach the correct chunker.
	if err := m.table.AttachChunker(m.optTargetChunkMs, m.optDisableTrivialChunker, m.logger); err != nil {
		return err
	}
	// This step is technically optional, but first we attempt to
	// use MySQL's built-in DDL. This is because it's usually faster
	// when it is compatible. If it returns no error, that means it
	// has been successful and the DDL is complete.
	err = m.attemptMySQLDDL()
	if err == nil {
		m.logger.WithFields(log.Fields{
			"instant-ddl": m.usedInstantDDL,
			"inplace-ddl": m.usedInplaceDDL,
		}).Info("apply complete")
		return nil // success!
	}
	// Perform preflight basic checks. These are features that are required
	// for the migration to proceed.
	if err := m.preflightChecks(); err != nil {
		return err
	}

	// Perform setup steps, including resuming from a checkpoint (if available)
	// and creating the shadow and checkpoint tables.
	// The replication client is also created here.
	if err := m.setup(); err != nil {
		return err
	}

	go m.dumpStatus()                        // start periodically writing status
	go m.dumpCheckpointContinuously()        // start periodically dumping the checkpoint.
	go m.updateTableStatisticsContinuously() // update the min/max and estimated rows.
	go m.periodicFlush(ctx)                  // advance the binary log position periodically.

	// Perform the main copy rows task. This is where the majority
	// of migrations usually spend time.
	m.setCurrentState(migrationStateCopyRows)
	if err := m.copier.Run(ctx); err != nil {
		return err
	}
	// This step is technically optional, but skipping it might cause the next steps
	// to block for more time. Once the copy-rows task is complete, we try
	// to catch up on the binary log as much as possible before proceeding.
	m.setCurrentState(migrationStateApplyChangeset)
	m.periodicFlushLock.Lock() // Wait for the periodic flush to finish.
	if err := m.feed.FlushUntilTrivial(ctx); err != nil {
		return err
	}
	m.periodicFlushLock.Unlock() // It will not start again because the current state is now ApplyChangeset.

	// The checksum is optional, but it is ONLINE after an initial lock
	// for consistency. It is the main way that we determine that
	// this program is safe to use even when immature.
	if m.optChecksum {
		m.setCurrentState(migrationStateChecksum)
		if err := m.checksum(ctx); err != nil {
			return err
		}
	}
	// The checksum passes! It's time for the final cut-over, where
	// the tables are swapped under a lock.
	m.setCurrentState(migrationStateCutOver)
	cutover, err := NewCutOver(m.db, m.table, m.shadowTable, m.feed, m.logger)
	if err != nil {
		return err
	}
	if err := cutover.Run(ctx); err != nil {
		return err
	}
	m.logger.WithFields(log.Fields{
		"instant-ddl":  m.usedInstantDDL,
		"inplace-ddl":  m.usedInplaceDDL,
		"total-chunks": m.copier.CopyChunksCount,
		"duration":     time.Since(m.startTime).String(),
	}).Info("apply complete")
	return nil
}

// attemptMySQLDDL "attempts" to use DDL directly on MySQL with an assertion
// such as ALGORITHM=INSTANT. If MySQL is able to use the INSTANT algorithm,
// it will perform the operation without error. If it can't, it will return
// an error. It is important to let MySQL decide if it can handle the DDL
// operation, because keeping track of which operations are "INSTANT"
// is incredibly difficult. It will depend on MySQL minor version,
// and could possibly be specific to the table.
func (m *MigrationRunner) attemptMySQLDDL() error {
	err := m.attemptInstantDDL()
	if err == nil {
		m.usedInstantDDL = true // success
		return nil
	}
	// Inplace DDL is feature gated because it blocks replicas.
	// It's only safe to do in aurora GLOBAL because replicas do not
	// use the binlog.
	if m.optAttemptInplaceDDL {
		err = m.attemptInplaceDDL()
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

func (m *MigrationRunner) dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", m.username, m.password, m.host, m.schemaName)
}

func (m *MigrationRunner) setup() error {
	// Drop the old table. It shouldn't exist, but it could.
	if err := m.dropOldTable(); err != nil {
		return err
	}

	// First attempt to resume from a checkpoint.
	// It's OK if it fails, it just means it's a fresh migration.
	if err := m.resumeFromCheckpoint(); err != nil {
		// Resume failed, do the initial steps.
		m.logger.Info("could not resume from checkpoint")
		if err := m.createShadowTable(); err != nil {
			return err
		}
		if err := m.alterShadowTable(); err != nil {
			return err
		}
		if err := m.createCheckpointTable(); err != nil {
			return err
		}
		if err := m.table.Chunker.Open(); err != nil {
			return err
		}
		m.feed = repl.NewClient(m.db, m.host, m.table, m.shadowTable, m.username, m.password, m.logger)
		m.copier, err = copier.NewCopier(m.db, m.table, m.shadowTable, m.optConcurrency, m.optChecksum, m.logger)
		if err != nil {
			return err
		}
		// Start the binary log feed now
		if err := m.feed.Run(); err != nil {
			return err
		}
	}

	// If the replica DSN was specified, attach a replication throttler.
	// Otherwise it will default to the NOOP throttler.
	if m.optReplicaDSN != "" {
		replica, err := sql.Open("mysql", m.optReplicaDSN)
		if err != nil {
			return err
		}
		mythrottler, err := throttler.NewReplicationThrottler(replica, m.optReplicaMaxLagMs, m.logger)
		if err != nil {
			m.copier.SetThrottler(mythrottler)
			if err := mythrottler.Start(); err != nil {
				return err
			}
		}
	}

	// Make sure the definition of the table never changes.
	// If it does, we could be in trouble.
	m.feed.TableChangeNotificationCallback = m.tableChangeNotification

	return nil
}

func (m *MigrationRunner) tableChangeNotification() {
	// It's an async message, so we don't know the current state
	// from which this "notification" was generated, but typically if our
	// current state is now in cutover, we can ignore it.
	if m.getCurrentState() >= migrationStateCutOver {
		return
	}
	m.setCurrentState(migrationStateErrCleanup)
	// Write this to the logger, so it can be captured by the initiator.
	m.logger.Error("table definition changed during migration")
	// Invalidate the checkpoint, so we don't try to resume.
	// If we don't do this, the migration will permanently be blocked from proceeding.
	// Letting it start again is the better choice.
	if err := m.dropCheckpoint(); err != nil {
		m.logger.WithError(err).Error("could not remove checkpoint")
	}
	// We can't do anything about it, just panic
	panic("table definition changed during migration")
}

func (m *MigrationRunner) dropCheckpoint() error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", m.checkpointTable.QuotedName())
	_, err := m.db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func (m *MigrationRunner) createShadowTable() error {
	shadowName := fmt.Sprintf("_%s_shadow", m.table.TableName)
	if len(shadowName) > 64 {
		return fmt.Errorf("table name is too long: '%s'. shadow table name will exceed 64 characters", m.table.TableName)
	}
	// drop both if we've decided to call this func.
	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", m.table.SchemaName, shadowName)
	_, err := m.db.Exec(query)
	if err != nil {
		return err
	}
	query = fmt.Sprintf("CREATE TABLE `%s`.`%s` LIKE %s", m.table.SchemaName, shadowName, m.table.QuotedName())
	_, err = m.db.Exec(query)
	if err != nil {
		return err
	}
	m.shadowTable = table.NewTableInfo(m.schemaName, shadowName)
	if err := m.shadowTable.RunDiscovery(m.db); err != nil {
		return err
	}
	return nil
}

// TODO: should we check for DROP and ADD of the same name?
// This would intersect as true, but semantically that is not the correct behavior.
func (m *MigrationRunner) checkAlterTableIsNotRename(sql string) error {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return fmt.Errorf("could not parse alter table statement: %s", sql)
	}
	stmt := &stmtNodes[0]
	alterStmt, ok := (*stmt).(*ast.AlterTableStmt)
	if !ok {
		return fmt.Errorf("not a valid alter table statement")
	}
	for _, spec := range alterStmt.Specs {
		if spec.Tp == ast.AlterTableRenameTable || spec.Tp == ast.AlterTableRenameColumn {
			return fmt.Errorf("renames are not supported by the shadow table algorithm")
		}
		// ALTER TABLE CHANGE COLUMN can be used to rename a column.
		// But they can also be used commonly without a rename, so the check needs to be deeper.
		if spec.Tp == ast.AlterTableChangeColumn {
			if spec.NewColumns[0].Name.String() != spec.OldColumnName.String() {
				return fmt.Errorf("renames are not supported by the shadow table algorithm")
			}
		}
	}
	return nil // no renames
}

// alterShadowTable uses the TiDB parser to preflight check that the alter statement is not a rename
// We only need to check here because it breaks this algorithm. In most cases,
// renames work with INSTANT ddl so this code is not required. The typical
// case where it is required is multiple changes in one alter and one is a rename.
func (m *MigrationRunner) alterShadowTable() error {
	query := fmt.Sprintf("ALTER TABLE %s %s", m.shadowTable.QuotedName(), m.alterStatement)
	if err := m.checkAlterTableIsNotRename(query); err != nil {
		return err
	}
	_, err := m.db.Exec(query)
	return err
}

func (m *MigrationRunner) dropOldTable() error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", m.table.SchemaName, m.table.TableName+"_old")
	_, err := m.db.Exec(query)
	return err
}

func (m *MigrationRunner) attemptInstantDDL() error {
	query := fmt.Sprintf("ALTER TABLE %s %s, ALGORITHM=INSTANT", m.table.QuotedName(), m.alterStatement)
	_, err := m.db.Exec(query)
	return err
}

func (m *MigrationRunner) attemptInplaceDDL() error {
	query := fmt.Sprintf("ALTER TABLE %s %s, ALGORITHM=INPLACE, LOCK=NONE", m.table.QuotedName(), m.alterStatement)
	_, err := m.db.Exec(query)
	return err
}

func (m *MigrationRunner) createCheckpointTable() error {
	cpName := fmt.Sprintf("_%s_cp", m.table.TableName)
	// drop both if we've decided to call this func.
	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", m.table.SchemaName, cpName)
	_, err := m.db.Exec(query)
	if err != nil {
		return err
	}
	query = fmt.Sprintf("CREATE TABLE `%s`.`%s` (id int NOT NULL AUTO_INCREMENT PRIMARY KEY, copy_rows_at TEXT, binlog_name VARCHAR(255), binlog_pos INT, copy_rows BIGINT, alter_statement TEXT)",
		m.table.SchemaName, cpName)
	_, err = m.db.Exec(query)
	if err != nil {
		return err
	}
	m.checkpointTable = table.NewTableInfo(m.table.SchemaName, cpName)
	if err != nil {
		return err
	}
	return nil
}

func (m *MigrationRunner) Close() error {
	m.setCurrentState(migrationStateClose)
	if m.shadowTable == nil {
		return nil
	}
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", m.shadowTable.QuotedName())
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
	if m.feed != nil {
		m.feed.Close()
	}
	return m.db.Close()
}

func (m *MigrationRunner) preflightChecks() error {
	var binlogFormat, innodbAutoincLockMode, binlogRowImage string
	err := m.db.QueryRow("SELECT @@global.binlog_format, @@global.innodb_autoinc_lock_mode, @@global.binlog_row_image").Scan(&binlogFormat, &innodbAutoincLockMode, &binlogRowImage)
	if err != nil {
		return err
	}
	if binlogFormat != "ROW" {
		return fmt.Errorf("binlog_format must be ROW")
	}
	if innodbAutoincLockMode != "2" {
		// This is strongly encouraged because otherwise running parallel threads is pointless.
		// i.e. on a test with 2 threads running INSERT INTO new SELECT * FROM old WHERE <range>
		// the inserts will run in serial when there is an autoinc column on new and innodbAutoincLockMode != "2"
		// This is the auto-inc lock. It won't show up in SHOW PROCESSLIST that they are serial.
		m.logger.Warn("innodb_autoinc_lock_mode != 2. This will cause the migration to run slower than expected because concurrent inserts to the new table will be serialized.")
	}
	if binlogRowImage != "FULL" {
		// This might not be required, but is the only option that has been tested so far.
		// To keep the testing scope reduced for now, it is required.
		return fmt.Errorf("binlog_row_image must be FULL")
	}
	return nil
}

func (m *MigrationRunner) resumeFromCheckpoint() error {
	// Check that the shadow table exists and the checkpoint table
	// has at least one row in it.

	// The objects for these are not available until we confirm
	// tables exist and we
	shadowName := fmt.Sprintf("_%s_shadow", m.table.TableName)
	cpName := fmt.Sprintf("_%s_cp", m.table.TableName)

	// Make sure we can read from the shadow table.
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT 1",
		m.schemaName, shadowName)
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
		return fmt.Errorf("alter statement in checkpoint table does not match the alter statement specified here")
	}
	// Populate the objects that would have been set in the other funcs.
	m.shadowTable = table.NewTableInfo(m.schemaName, shadowName)
	if err := m.shadowTable.RunDiscovery(m.db); err != nil {
		return err
	}

	// Set the binlog position.
	// Create a binlog subscriber
	m.feed = repl.NewClient(m.db, m.host, m.table, m.shadowTable, m.username, m.password, m.logger)

	m.feed.SetPos(&mysql.Position{
		Name: binlogName,
		Pos:  uint32(binlogPos),
	})

	m.checkpointTable = table.NewTableInfo(m.table.SchemaName, cpName)
	if err != nil {
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
	m.copier, err = copier.NewCopier(m.db, m.table, m.shadowTable, m.optConcurrency, m.optChecksum, m.logger)
	if err != nil {
		return err
	}
	// Start the feed now. This is because if the checkpoint is so old there
	// are no longer binary log files, we want to abandon resume-from-checkpoint
	// and still be able to start from scratch.
	// Start the binary log feed just before copy rows starts.
	if err := m.feed.Run(); err != nil {
		m.logger.WithFields(log.Fields{
			"log-file": binlogName,
			"log-pos":  binlogPos,
		}).Warn("resuming from checkpoint failed because resuming from the previous binlog position failed")
		return err
	}
	// Success from this point on
	// Overwrite copy-rows
	atomic.StoreInt64(&m.copier.CopyRowsCount, copyRows)

	// Open the table at a specific point.
	if err := m.table.Chunker.OpenAtWatermark(copyRowsAt); err != nil {
		return err // could not open table
	}

	m.logger.WithFields(log.Fields{
		"low-watermark": copyRowsAt,
		"log-file":      binlogName,
		"log-pos":       binlogPos,
		"copy-rows":     copyRows,
	}).Warn("resuming from checkpoint")
	return nil
}

// checksum creates the checksum which opens the read view.
func (m *MigrationRunner) checksum(ctx context.Context) error {
	// The chunker table.Chunker is tied into the checkpoint, so if we reset
	// it will lose our progress. I had considered adding a Reset() method
	// to the Chunker interface, but this complicates the code for an isolated use-case.
	// Instead we now create a new table4checker, new chunker and use that.
	table4checker := table.NewTableInfo(m.table.SchemaName, m.table.TableName)
	if err := table4checker.RunDiscovery(m.db); err != nil {
		return err
	}
	if err := table4checker.AttachChunker(m.optTargetChunkMs, m.optDisableTrivialChunker, m.logger); err != nil {
		return err
	}
	if err := table4checker.Chunker.Open(); err != nil {
		return err
	}
	checker, err := checksum.NewChecker(m.db, table4checker, m.shadowTable, m.optChecksumConcurrency, m.feed, m.logger)
	if err != nil {
		return err
	}
	if err := checker.Run(ctx); err != nil {
		return err
	}
	m.logger.Info("checksum passed")
	return nil
}

func (m *MigrationRunner) getCurrentState() migrationState {
	return migrationState(atomic.LoadInt32((*int32)(&m.currentState)))
}

func (m *MigrationRunner) setCurrentState(s migrationState) {
	atomic.StoreInt32((*int32)(&m.currentState), int32(s))
	if s > migrationStateCopyRows && m.feed != nil {
		m.feed.SetKeyAboveWatermarkOptimization(false)
	}
}

func (m *MigrationRunner) dumpCheckpoint() error {
	// Retrieve the binlog position first and under a mutex.
	// Currently it never advances but it's possible it might in future
	// and this race condition is missed.
	binlog := m.feed.GetBinlogApplyPosition()
	lowWatermark, err := m.table.Chunker.GetLowWatermark()
	if err != nil {
		return err // it might not be ready, we can try again.
	}
	copyRows := atomic.LoadInt64(&m.copier.CopyRowsCount)

	m.logger.WithFields(log.Fields{
		"low-watermark": lowWatermark,
		"log-file":      binlog.Name,
		"log-pos":       binlog.Pos,
		"copy-rows":     copyRows,
	}).Info("checkpoint")

	query := fmt.Sprintf("INSERT INTO %s (copy_rows_at, binlog_name, binlog_pos, copy_rows, alter_statement) VALUES (?, ?, ?, ?, ?)",
		m.checkpointTable.QuotedName())
	_, err = m.db.Exec(query, lowWatermark, binlog.Name, binlog.Pos, copyRows, m.alterStatement)
	return err
}

func (m *MigrationRunner) dumpCheckpointContinuously() {
	ticker := time.NewTicker(checkpointDumpInterval)
	defer ticker.Stop()
	for range ticker.C {
		// Continue to checkpoint until we exit copy-rows.
		// Ideally in future we can continue further than this,
		// but unfortunately this currently results in a
		// "watermark not ready" error.
		if m.getCurrentState() > migrationStateCopyRows {
			return
		}
		if err := m.dumpCheckpoint(); err != nil {
			m.logger.WithFields(log.Fields{
				"error": err,
			}).Error("error writing checkpoint")
		}
	}
}

func (m *MigrationRunner) periodicFlush(ctx context.Context) {
	ticker := time.NewTicker(binlogPerodicFlushInterval)
	defer ticker.Stop()
	for range ticker.C {
		// We only want to continuously flush during copy-rows.
		// During checkpoint we only lock the source table, so if we
		// are in a periodic flush we can't be writing data to the shadow table.
		// If we do, then we'll need to lock it as well so that the read-view is consistent.
		m.periodicFlushLock.Lock()
		if m.getCurrentState() > migrationStateCopyRows {
			m.periodicFlushLock.Unlock()
			return
		}
		startLoop := time.Now()
		m.logger.Info("starting periodic flush of binary log")
		// The periodic flush does not respect the throttler. It is only single-threaded
		// by design (chunked into 10K rows). Since we want to advance the binlog position
		// we allow this to run, and then expect that if it is under load the throttler
		// will kick in and slow down the copy-rows.
		if err := m.feed.Flush(ctx); err != nil {
			m.logger.WithFields(log.Fields{
				"error": err,
			}).Error("error flushing binary log")
		}
		m.periodicFlushLock.Unlock()
		m.logger.WithFields(log.Fields{
			"duration": time.Since(startLoop),
		}).Info("finished periodic flush of binary log")
	}
}

func (m *MigrationRunner) updateTableStatisticsContinuously() {
	ticker := time.NewTicker(tableStatUpdateInterval)
	defer ticker.Stop()
	for range ticker.C {
		if m.getCurrentState() > migrationStateCopyRows {
			return
		}
		if err := m.table.UpdateTableStatistics(m.db); err != nil {
			m.logger.WithFields(log.Fields{
				"error": err,
			}).Error("error updating table statistics")
		}
	}
}

func (m *MigrationRunner) dumpStatus() {
	go m.estimateRowsPerSecondLoop()

	ticker := time.NewTicker(statusInterval)
	defer ticker.Stop()
	for range ticker.C {
		if m.getCurrentState() > migrationStateCopyRows {
			return
		}
		pct := float64(atomic.LoadInt64(&m.copier.CopyRowsCount)) / float64(m.table.EstimatedRows) * 100
		m.logger.WithFields(log.Fields{
			"state":               m.getCurrentState().String(),
			"copy-progress":       fmt.Sprintf("%.2f%%", pct),
			"binlog-deltas":       m.feed.GetDeltaLen(),
			"time-total":          time.Since(m.startTime).String(),
			"eta":                 m.getETAFromRowsPerSecond(pct > 99.9),
			"copier-is-throttled": m.copier.Throttler.IsThrottled(),
		}).Info("migration status")
	}
}

func (m *MigrationRunner) estimateRowsPerSecondLoop() {
	// We take 10 second averages not 1 second
	// because with parallel copy it bounces around a lot.
	prevRowsCount := atomic.LoadInt64(&m.copier.CopyRowsCount)
	ticker := time.NewTicker(copyEstimateInterval)
	defer ticker.Stop()
	for range ticker.C {
		if m.getCurrentState() > migrationStateCopyRows {
			return
		}
		newRowsCount := atomic.LoadInt64(&m.copier.CopyRowsCount)
		rowsPerSecond := (newRowsCount - prevRowsCount) / 10
		atomic.StoreInt64(&m.copier.EtaRowsPerSecond, rowsPerSecond)
		prevRowsCount = newRowsCount
	}
}

func (m *MigrationRunner) getETAFromRowsPerSecond(due bool) string {
	rowsPerSecond := atomic.LoadInt64(&m.copier.EtaRowsPerSecond)
	if m.getCurrentState() > migrationStateCopyRows || due {
		return "Due" // in apply rows phase or checksum
	}
	if rowsPerSecond == 0 {
		return "-" // not enough data yet, or in last phase
	}

	remainingRows := m.table.EstimatedRows - uint64(atomic.LoadInt64(&m.copier.CopyRowsCount))
	remainingSeconds := math.Floor(float64(remainingRows) / float64(rowsPerSecond))

	// We could just return it as "12345 seconds" but to group it to hours/days.
	// We convert to time.Duration which will interpret this as nanoseconds,
	// so we need to multiply by seconds.
	return time.Duration(remainingSeconds * float64(time.Second)).String()
}

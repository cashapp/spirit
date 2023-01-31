package copier

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/squareup/gap-core/log"
	"github.com/squareup/spirit/pkg/table"
	"github.com/squareup/spirit/pkg/throttler"

	"github.com/stretchr/testify/assert"
)

const (
	TestHost     = "127.0.0.1:8030"
	TestSchema   = "test"
	TestUser     = "msandbox"
	TestPassword = "msandbox"
)

func dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", TestUser, TestPassword, TestHost, TestSchema)
}

func runSQL(t *testing.T, stmt string) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", TestUser, TestPassword, TestHost, TestSchema)
	db, err := sql.Open("mysql", dsn)
	assert.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(stmt)
	assert.NoError(t, err)
}

func TestCopier(t *testing.T) {
	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "INSERT INTO t1 VALUES (1, 2, 3)")

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	assert.NoError(t, t1.AttachChunker(100, true, nil))
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))

	logger := log.New(log.LoggingConfig{})
	copier, err := NewCopier(db, t1, t2, 0, true, logger)
	assert.NoError(t, err)
	assert.NoError(t, t1.Chunker.Open())
	assert.NoError(t, copier.Run(context.Background())) // works

	// Verify that t2 has one row.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM t2").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestThrottler(t *testing.T) {
	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "INSERT INTO t1 VALUES (1, 2, 3)")

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	assert.NoError(t, t1.AttachChunker(100, true, nil))
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))

	logger := log.New(log.LoggingConfig{})
	copier, err := NewCopier(db, t1, t2, 0, true, logger)
	assert.NoError(t, err)
	assert.NoError(t, t1.Chunker.Open())
	copier.SetThrottler(&throttler.Noop{})
	assert.NoError(t, copier.Run(context.Background())) // works

	// Verify that t2 has one row.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM t2").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestCopierUniqueDestination(t *testing.T) {
	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a), UNIQUE(b))")
	runSQL(t, "INSERT INTO t1 VALUES (1, 2, 3), (2,2,3)")

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	assert.NoError(t, t1.AttachChunker(100, true, nil))
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))

	// if the checksum is FALSE, the unique violation will cause an error.
	logger := log.New(log.LoggingConfig{})
	copier, err := NewCopier(db, t1, t2, 0, false, logger)
	assert.NoError(t, err)
	assert.NoError(t, t1.Chunker.Open())
	assert.Error(t, copier.Run(context.Background())) // fails

	// however, if the checksum is TRUE, the unique violation will be ignored.
	// This is because it's not possible to differentiate between a resume from checkpoint
	// causing a duplicate key, and the DDL being applied causing it.
	t1 = table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	assert.NoError(t, t1.AttachChunker(100, true, nil))
	t2 = table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))
	copier, err = NewCopier(db, t1, t2, 0, true, logger)
	assert.NoError(t, err)
	assert.NoError(t, t1.Chunker.Open())
	assert.NoError(t, copier.Run(context.Background())) // works
}

func TestCopierLossyDataTypeConversion(t *testing.T) {
	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL, b INT, c VARCHAR(255), PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "INSERT INTO t1 VALUES (1, 2, 'aaa'), (2,2,'bbb')")

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	assert.NoError(t, t1.AttachChunker(100, true, nil))
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))

	// Checksum flag does not affect this error.
	logger := log.New(log.LoggingConfig{})
	copier, err := NewCopier(db, t1, t2, 0, true, logger)
	assert.NoError(t, err)
	assert.NoError(t, t1.Chunker.Open())
	err = copier.Run(context.Background())
	assert.Contains(t, err.Error(), "unsafe warning migrating chunk")
}

func TestCopierNullToNotNullConversion(t *testing.T) {
	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL, b INT, c INT NOT NULL, PRIMARY KEY (a))")
	runSQL(t, "INSERT INTO t1 VALUES (1, 2, 123), (2,2,NULL)")

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	assert.NoError(t, t1.AttachChunker(100, true, nil))
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))

	// Checksum flag does not affect this error.
	logger := log.New(log.LoggingConfig{})
	copier, err := NewCopier(db, t1, t2, 0, true, logger)
	assert.NoError(t, err)
	assert.NoError(t, t1.Chunker.Open())
	err = copier.Run(context.Background())
	assert.Contains(t, err.Error(), "unsafe warning migrating chunk")
}

func TestSQLModeAllowZeroInvalidDates(t *testing.T) {
	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL, b INT, c DATETIME, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL, b INT, c DATETIME, PRIMARY KEY (a))")
	runSQL(t, "INSERT IGNORE INTO t1 VALUES (1, 2, '0000-00-00')")

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	assert.NoError(t, t1.AttachChunker(100, true, nil))
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))

	// Checksum flag does not affect this error.
	logger := log.New(log.LoggingConfig{})
	copier, err := NewCopier(db, t1, t2, 0, true, logger)
	assert.NoError(t, err)
	assert.NoError(t, t1.Chunker.Open())
	err = copier.Run(context.Background())
	assert.NoError(t, err)
	// Verify that t2 has one row.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM t2").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestLockWaitTimeoutIsRetyable(t *testing.T) {
	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "INSERT IGNORE INTO t1 VALUES (1, 2, 3)")

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	assert.NoError(t, t1.AttachChunker(100, true, nil))
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))

	// Lock table t2 for 2 seconds.
	// This should be enough to retry, but it will eventually be successful.
	go func() {
		tx, err := db.Begin()
		assert.NoError(t, err)
		_, err = tx.Exec("SELECT * FROM t2 WHERE a = 1 FOR UPDATE")
		assert.NoError(t, err)
		time.Sleep(2 * time.Second)
		err = tx.Rollback()
		assert.NoError(t, err)
	}()
	logger := log.New(log.LoggingConfig{})
	copier, err := NewCopier(db, t1, t2, 0, true, logger)
	assert.NoError(t, err)
	assert.NoError(t, t1.Chunker.Open())
	err = copier.Run(context.Background())
	assert.NoError(t, err) // succeeded within retry.
}

func TestLockWaitTimeoutRetryExceeded(t *testing.T) {
	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "INSERT IGNORE INTO t1 VALUES (1, 2, 3)")

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	assert.NoError(t, t1.AttachChunker(100, true, nil))
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))

	// Lock again but for 10 seconds.
	// This will cause a failure.
	go func() {
		tx, err := db.Begin()
		assert.NoError(t, err)
		_, err = tx.Exec("SELECT * FROM t2 WHERE a = 1 FOR UPDATE")
		assert.NoError(t, err)
		time.Sleep(10 * time.Second)
		err = tx.Rollback()
		assert.NoError(t, err)
	}()

	logger := log.New(log.LoggingConfig{})
	copier, err := NewCopier(db, t1, t2, 0, true, logger)
	assert.NoError(t, err)
	assert.NoError(t, t1.Chunker.Open())
	err = copier.Run(context.Background())
	assert.Error(t, err) // exceeded retry.
}

func TestCopierValidation(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	t1 := table.NewTableInfo("test", "t1")
	t2 := table.NewTableInfo("test", "t2")

	// if the checksum is FALSE, the unique violation will cause an error.
	logger := log.New(log.LoggingConfig{})
	_, err = NewCopier(db, t1, nil, 0, false, logger)
	assert.Error(t, err)
	_, err = NewCopier(db, t1, t2, 0, false, logger)
	assert.Error(t, err) // no chunker attached
}

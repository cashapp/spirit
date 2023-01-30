package checksum

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/stretchr/testify/assert"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/spirit/pkg/repl"
	"github.com/squareup/spirit/pkg/table"
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

func TestBasicChecksum(t *testing.T) {
	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "INSERT INTO t1 VALUES (1, 2, 3)")
	runSQL(t, "INSERT INTO t2 VALUES (1, 2, 3)")

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	assert.NoError(t, t1.AttachChunker(100, true, nil))
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))
	assert.NoError(t, t1.Chunker.Open())
	logger := log.New()
	feed := repl.NewClient(db, TestHost, t1, t2, TestUser, TestPassword, logger)
	assert.NoError(t, feed.Run())

	checker, err := NewChecker(db, t1, t2, 4, feed, logger)
	assert.NoError(t, err)
	assert.NoError(t, checker.Run(context.Background()))
}

func TestBasicValidation(t *testing.T) {
	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "INSERT INTO t1 VALUES (1, 2, 3)")
	runSQL(t, "INSERT INTO t2 VALUES (1, 2, 3)")

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))
	logger := log.New()
	feed := repl.NewClient(db, TestHost, t1, t2, TestUser, TestPassword, logger)
	assert.NoError(t, feed.Run())

	_, err = NewChecker(db, nil, t2, 0, feed, logger)
	assert.EqualError(t, err, "table and shadowTable must be non-nil")
	_, err = NewChecker(db, t1, nil, 0, feed, logger)
	assert.EqualError(t, err, "table and shadowTable must be non-nil")
	_, err = NewChecker(db, t1, t2, 0, feed, logger)
	assert.EqualError(t, err, "table must have chunker attached")
	assert.NoError(t, t1.AttachChunker(100, true, nil))
	_, err = NewChecker(db, t1, t2, 0, feed, logger)
	assert.NoError(t, err)
	_, err = NewChecker(db, t1, t2, 0, nil, logger) // no feed
	assert.EqualError(t, err, "feed must be non-nil")
}

func TestCorruptChecksum(t *testing.T) {
	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "INSERT INTO t1 VALUES (1, 2, 3)")
	runSQL(t, "INSERT INTO t2 VALUES (1, 2, 3)")
	runSQL(t, "INSERT INTO t2 VALUES (2, 2, 3)") // corrupt

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	assert.NoError(t, t1.AttachChunker(100, true, nil))
	assert.NoError(t, t1.Chunker.Open())
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))
	logger := log.New()
	feed := repl.NewClient(db, TestHost, t1, t2, TestUser, TestPassword, logger)
	assert.NoError(t, feed.Run())

	checker, err := NewChecker(db, t1, t2, 4, feed, logger)
	assert.NoError(t, err)
	err = checker.Run(context.Background())
	assert.ErrorContains(t, err, "checksum mismatch")
}

func TestBoundaryCases(t *testing.T) {
	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL, b FLOAT, c VARCHAR(255), PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL, b FLOAT, c VARCHAR(255), PRIMARY KEY (a))")
	runSQL(t, "INSERT INTO t1 VALUES (1, 2.2, '')")   // null vs empty string
	runSQL(t, "INSERT INTO t2 VALUES (1, 2.2, NULL)") // should not compare

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	assert.NoError(t, t1.AttachChunker(100, true, nil))
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))
	logger := log.New()
	feed := repl.NewClient(db, TestHost, t1, t2, TestUser, TestPassword, logger)
	assert.NoError(t, feed.Run())

	checker, err := NewChecker(db, t1, t2, 4, feed, logger)
	assert.NoError(t, err)
	assert.Error(t, checker.Run(context.Background()))

	// UPDATE t1 to also be NULL
	runSQL(t, "UPDATE t1 SET c = NULL")
	assert.NoError(t, t1.Chunker.Open())
	checker, err = NewChecker(db, t1, t2, 4, feed, logger)
	assert.NoError(t, err)
	assert.NoError(t, checker.Run(context.Background()))
}

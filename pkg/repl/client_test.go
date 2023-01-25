package repl

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"

	"github.com/squareup/gap-core/log"
	"github.com/squareup/spirit/pkg/table"
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
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(stmt)
	assert.NoError(t, err)
}

func TestReplClient(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))

	logger := log.New(log.LoggingConfig{})
	client := NewClient(db, TestHost, t1, t2, TestUser, TestPassword, logger)
	assert.NoError(t, client.Run())

	// Insert into t1.
	runSQL(t, "INSERT INTO t1 (a, b, c) VALUES (1, 2, 3)")
	assert.NoError(t, client.BlockWait())
	// There is no chunker attached, so the key above watermark can't apply.
	// We should observe there are now rows in the changeset.
	assert.Equal(t, client.GetDeltaLen(), 1)
	assert.NoError(t, client.FlushUntilTrivial(context.TODO()))

	// We should observe there is a row in t2.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM t2").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestReplClientComplex(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL auto_increment, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL  auto_increment, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "INSERT INTO t1 (a, b, c) SELECT NULL, 1, 1 FROM dual")
	runSQL(t, "INSERT INTO t1 (a, b, c) SELECT NULL, 1, 1 FROM t1 a JOIN t1 b JOIN t1 c LIMIT 100000")
	runSQL(t, "INSERT INTO t1 (a, b, c) SELECT NULL, 1, 1 FROM t1 a JOIN t1 b JOIN t1 c LIMIT 100000")
	runSQL(t, "INSERT INTO t1 (a, b, c) SELECT NULL, 1, 1 FROM t1 a JOIN t1 b JOIN t1 c LIMIT 100000")
	runSQL(t, "INSERT INTO t1 (a, b, c) SELECT NULL, 1, 1 FROM t1 a JOIN t1 b JOIN t1 c LIMIT 100000")

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	assert.NoError(t, t1.AttachChunker(100, true, nil))
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))

	logger := log.New(log.LoggingConfig{})
	client := NewClient(db, TestHost, t1, t2, TestUser, TestPassword, logger)
	assert.NoError(t, client.Run())

	// Insert into t1, but because there is no read yet, the key is above the watermark
	runSQL(t, "DELETE FROM t1 WHERE a BETWEEN 10 and 500")
	assert.NoError(t, client.BlockWait())
	assert.Equal(t, client.GetDeltaLen(), 0)

	// Read from the chunker so that the key is below the watermark
	assert.NoError(t, t1.Chunker.Open())
	chk, err := t1.Chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, chk.String(), "a < 1")
	// read again
	chk, err = t1.Chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, chk.String(), "a >= 1 AND a < 1001")

	// Now if we delete below 1001 we should see 10 deltas accumulate
	runSQL(t, "DELETE FROM t1 WHERE a >= 550 AND a < 560")
	assert.NoError(t, client.BlockWait())
	assert.Equal(t, 10, client.GetDeltaLen()) // 10 keys did not exist on t1

	// Flush the changeset
	assert.NoError(t, client.Flush(context.TODO()))

	// Accumulate more deltas
	runSQL(t, "DELETE FROM t1 WHERE a >= 550 AND a < 570")
	assert.NoError(t, client.BlockWait())
	assert.Equal(t, 10, client.GetDeltaLen()) // 10 keys did not exist on t1
	runSQL(t, "UPDATE t1 SET b = 213 WHERE a >= 550 AND a < 1001")
	assert.NoError(t, client.BlockWait())
	assert.Equal(t, 441, client.GetDeltaLen()) // ??

	// Final flush
	assert.NoError(t, client.FlushUntilTrivial(context.TODO()))

	// We should observe there is a row in t2.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM t2").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 431, count) // 441 - 10
}

func TestReplClientResumeFromImpossible(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))

	logger := log.New(log.LoggingConfig{})
	client := NewClient(db, TestHost, t1, t2, TestUser, TestPassword, logger)
	client.SetStartingPos(&mysql.Position{
		Name: "impossible",
		Pos:  uint32(12345),
	})
	err = client.Run()
	assert.Error(t, err)
}

func TestReplClientResumeFromPoint(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))

	logger := log.New(log.LoggingConfig{})
	client := NewClient(db, TestHost, t1, t2, TestUser, TestPassword, logger)
	pos, err := client.getCurrentBinlogPosition()
	assert.NoError(t, err)
	pos.Pos = 4
	err = client.Run()
	assert.NoError(t, err)
}

func TestReplClientOpts(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	runSQL(t, "DROP TABLE IF EXISTS t1, t2")
	runSQL(t, "CREATE TABLE t1 (a INT NOT NULL auto_increment, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE t2 (a INT NOT NULL  auto_increment, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "INSERT INTO t1 (a, b, c) SELECT NULL, 1, 1 FROM dual")
	runSQL(t, "INSERT INTO t1 (a, b, c) SELECT NULL, 1, 1 FROM t1 a JOIN t1 b JOIN t1 c LIMIT 100000")
	runSQL(t, "INSERT INTO t1 (a, b, c) SELECT NULL, 1, 1 FROM t1 a JOIN t1 b JOIN t1 c LIMIT 100000")
	runSQL(t, "INSERT INTO t1 (a, b, c) SELECT NULL, 1, 1 FROM t1 a JOIN t1 b JOIN t1 c LIMIT 100000")
	runSQL(t, "INSERT INTO t1 (a, b, c) SELECT NULL, 1, 1 FROM t1 a JOIN t1 b JOIN t1 c LIMIT 100000")

	t1 := table.NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(db))
	assert.NoError(t, t1.AttachChunker(100, true, nil))
	t2 := table.NewTableInfo("test", "t2")
	assert.NoError(t, t2.RunDiscovery(db))

	logger := log.New(log.LoggingConfig{})
	client := NewClient(db, TestHost, t1, t2, TestUser, TestPassword, logger)
	assert.NoError(t, client.Run())

	// Disable key above watermark.
	client.SetKeyAboveWatermarkOptimization(false)

	startingPos := client.GetBinlogApplyPosition()

	// Delete more than 10000 keys so the FLUSH has to run in chunks.
	runSQL(t, "DELETE FROM t1 WHERE a BETWEEN 10 and 50000")
	assert.NoError(t, client.BlockWait())
	assert.Equal(t, client.GetDeltaLen(), 49961)

	// Flush
	assert.NoError(t, client.Flush(context.TODO()))
	assert.Equal(t, client.GetDeltaLen(), 0)

	// Currently the binlog never advances, this might change in the future.
	assert.Equal(t, startingPos, client.GetBinlogApplyPosition())
}

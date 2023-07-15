package repl

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"

	"github.com/squareup/spirit/pkg/row"
	"github.com/squareup/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
)

func dsn() string {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return "msandbox:msandbox@tcp(127.0.0.1:8030)/test"
	}
	return dsn
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

	runSQL(t, "DROP TABLE IF EXISTS replt1, replt2, _replt1_chkpnt")
	runSQL(t, "CREATE TABLE replt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE replt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE _replt1_chkpnt (a int)") // just used to advance binlog

	t1 := table.NewTableInfo(db, "test", "replt1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "replt2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	logger := logrus.New()
	cfg, err := mysql2.ParseDSN(dsn())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, t1, t2, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:      logger,
		Concurrency: 4,
		BatchSize:   10000,
	})
	assert.NoError(t, client.Run())
	defer client.Close()

	// Insert into t1.
	runSQL(t, "INSERT INTO replt1 (a, b, c) VALUES (1, 2, 3)")
	assert.NoError(t, client.BlockWait(context.TODO()))
	// There is no chunker attached, so the key above watermark can't apply.
	// We should observe there are now rows in the changeset.
	assert.Equal(t, client.GetDeltaLen(), 1)
	assert.NoError(t, client.Flush(context.TODO()))

	// We should observe there is a row in t2.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM replt2").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestReplClientComplex(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	runSQL(t, "DROP TABLE IF EXISTS replcomplext1, replcomplext2, _replcomplext1_chkpnt")
	runSQL(t, "CREATE TABLE replcomplext1 (a INT NOT NULL auto_increment, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE replcomplext2 (a INT NOT NULL  auto_increment, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE _replcomplext1_chkpnt (a int)") // just used to advance binlog

	runSQL(t, "INSERT INTO replcomplext1 (a, b, c) SELECT NULL, 1, 1 FROM dual")
	runSQL(t, "INSERT INTO replcomplext1 (a, b, c) SELECT NULL, 1, 1 FROM replcomplext1 a JOIN replcomplext1 b JOIN replcomplext1 c LIMIT 100000")
	runSQL(t, "INSERT INTO replcomplext1 (a, b, c) SELECT NULL, 1, 1 FROM replcomplext1 a JOIN replcomplext1 b JOIN replcomplext1 c LIMIT 100000")
	runSQL(t, "INSERT INTO replcomplext1 (a, b, c) SELECT NULL, 1, 1 FROM replcomplext1 a JOIN replcomplext1 b JOIN replcomplext1 c LIMIT 100000")
	runSQL(t, "INSERT INTO replcomplext1 (a, b, c) SELECT NULL, 1, 1 FROM replcomplext1 a JOIN replcomplext1 b JOIN replcomplext1 c LIMIT 100000")

	t1 := table.NewTableInfo(db, "test", "replcomplext1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "replcomplext2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	cfg, err := mysql2.ParseDSN(dsn())
	assert.NoError(t, err)

	client := NewClient(db, cfg.Addr, t1, t2, cfg.User, cfg.Passwd, NewClientDefaultConfig())
	assert.NoError(t, client.Run())
	defer client.Close()

	copier, err := row.NewCopier(db, t1, t2, row.NewCopierDefaultConfig())
	assert.NoError(t, err)
	// Attach copier's keyabovewatermark to the repl client
	client.KeyAboveCopierCallback = copier.KeyAboveHighWatermark

	assert.NoError(t, copier.Open4Test()) // need to manually open because we are not calling Run()

	// Insert into t1, but because there is no read yet, the key is above the watermark
	runSQL(t, "DELETE FROM replcomplext1 WHERE a BETWEEN 10 and 500")
	assert.NoError(t, client.BlockWait(context.TODO()))
	assert.Equal(t, client.GetDeltaLen(), 0)

	// Read from the copier so that the key is below the watermark
	chk, err := copier.Next4Test()
	assert.NoError(t, err)
	assert.Equal(t, "`a` < 1", chk.String())
	// read again
	chk, err = copier.Next4Test()
	assert.NoError(t, err)
	assert.Equal(t, "`a` >= 1 AND `a` < 1001", chk.String())

	// Now if we delete below 1001 we should see 10 deltas accumulate
	runSQL(t, "DELETE FROM replcomplext1 WHERE a >= 550 AND a < 560")
	assert.NoError(t, client.BlockWait(context.TODO()))
	assert.Equal(t, 10, client.GetDeltaLen()) // 10 keys did not exist on t1

	// Flush the changeset
	assert.NoError(t, client.Flush(context.TODO()))

	// Accumulate more deltas
	runSQL(t, "DELETE FROM replcomplext1 WHERE a >= 550 AND a < 570")
	assert.NoError(t, client.BlockWait(context.TODO()))
	assert.Equal(t, 10, client.GetDeltaLen()) // 10 keys did not exist on t1
	runSQL(t, "UPDATE replcomplext1 SET b = 213 WHERE a >= 550 AND a < 1001")
	assert.NoError(t, client.BlockWait(context.TODO()))
	assert.Equal(t, 441, client.GetDeltaLen()) // ??

	// Final flush
	assert.NoError(t, client.Flush(context.TODO()))

	// We should observe there is a row in t2.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM replcomplext2").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 431, count) // 441 - 10
}

func TestReplClientResumeFromImpossible(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	runSQL(t, "DROP TABLE IF EXISTS replresumet1, replresumet2, _replresumet1_chkpnt")
	runSQL(t, "CREATE TABLE replresumet1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE replresumet2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE _replresumet1_chkpnt (a int)") // just used to advance binlog

	t1 := table.NewTableInfo(db, "test", "replresumet1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "replresumet2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	logger := logrus.New()
	cfg, err := mysql2.ParseDSN(dsn())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, t1, t2, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:      logger,
		Concurrency: 4,
		BatchSize:   10000,
	})
	client.SetPos(mysql.Position{
		Name: "impossible",
		Pos:  uint32(12345),
	})
	err = client.Run()
	assert.Error(t, err)
}

func TestReplClientResumeFromPoint(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	runSQL(t, "DROP TABLE IF EXISTS replresumepointt1, replresumepointt2")
	runSQL(t, "CREATE TABLE replresumepointt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE replresumepointt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "replresumepointt1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "replresumepointt2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	logger := logrus.New()
	cfg, err := mysql2.ParseDSN(dsn())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, t1, t2, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:      logger,
		Concurrency: 4,
		BatchSize:   10000,
	})
	pos, err := client.getCurrentBinlogPosition()
	assert.NoError(t, err)
	pos.Pos = 4
	assert.NoError(t, client.Run())
	client.Close()
}

func TestReplClientOpts(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	runSQL(t, "DROP TABLE IF EXISTS replclientoptst1, replclientoptst2, _replclientoptst1_chkpnt")
	runSQL(t, "CREATE TABLE replclientoptst1 (a INT NOT NULL auto_increment, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE replclientoptst2 (a INT NOT NULL  auto_increment, b INT, c INT, PRIMARY KEY (a))")
	runSQL(t, "CREATE TABLE _replclientoptst1_chkpnt (a int)") // just used to advance binlog

	runSQL(t, "INSERT INTO replclientoptst1 (a, b, c) SELECT NULL, 1, 1 FROM dual")
	runSQL(t, "INSERT INTO replclientoptst1 (a, b, c) SELECT NULL, 1, 1 FROM replclientoptst1 a JOIN replclientoptst1 b JOIN replclientoptst1 c LIMIT 100000")
	runSQL(t, "INSERT INTO replclientoptst1 (a, b, c) SELECT NULL, 1, 1 FROM replclientoptst1 a JOIN replclientoptst1 b JOIN replclientoptst1 c LIMIT 100000")
	runSQL(t, "INSERT INTO replclientoptst1 (a, b, c) SELECT NULL, 1, 1 FROM replclientoptst1 a JOIN replclientoptst1 b JOIN replclientoptst1 c LIMIT 100000")
	runSQL(t, "INSERT INTO replclientoptst1 (a, b, c) SELECT NULL, 1, 1 FROM replclientoptst1 a JOIN replclientoptst1 b JOIN replclientoptst1 c LIMIT 100000")

	t1 := table.NewTableInfo(db, "test", "replclientoptst1")
	assert.NoError(t, t1.SetInfo(context.TODO()))
	t2 := table.NewTableInfo(db, "test", "replclientoptst2")
	assert.NoError(t, t2.SetInfo(context.TODO()))

	logger := logrus.New()
	cfg, err := mysql2.ParseDSN(dsn())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, t1, t2, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:      logger,
		Concurrency: 4,
		BatchSize:   10000,
	})
	assert.NoError(t, client.Run())
	defer client.Close()

	// Disable key above watermark.
	client.SetKeyAboveWatermarkOptimization(false)

	startingPos := client.GetBinlogApplyPosition()

	// Delete more than 10000 keys so the FLUSH has to run in chunks.
	runSQL(t, "DELETE FROM replclientoptst1 WHERE a BETWEEN 10 and 50000")
	assert.NoError(t, client.BlockWait(context.TODO()))
	assert.Equal(t, client.GetDeltaLen(), 49961)
	// Flush. We could use client.Flush() but for testing purposes lets use
	// PeriodicFlush()
	go client.StartPeriodicFlush(context.TODO(), 1*time.Second)
	time.Sleep(2 * time.Second)
	client.StopPeriodicFlush()

	assert.Equal(t, client.GetDeltaLen(), 0)

	// The binlog position should have changed.
	assert.NotEqual(t, startingPos, client.GetBinlogApplyPosition())
}

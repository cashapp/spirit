package migration

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/cashapp/spirit/pkg/dbconn"
	"github.com/cashapp/spirit/pkg/repl"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func dsn() string {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return "msandbox:msandbox@tcp(127.0.0.1:8030)/test"
	}
	return dsn
}

func TestCutOver(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS cutovert1, _cutovert1_new, _cutovert1_old, _cutovert1_chkpnt`)
	tbl := `CREATE TABLE cutovert1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, tbl)
	tbl = `CREATE TABLE _cutovert1_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, tbl)
	runSQL(t, `CREATE TABLE _cutovert1_chkpnt (a int)`) // for binlog advancement

	// The structure is the same, but insert 2 rows in t1 so
	// we can differentiate after the cutover.
	runSQL(t, `INSERT INTO cutovert1 VALUES (1, 2), (2,2)`)

	db, err := dbconn.New(dsn(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	assert.Equal(t, 0, db.Stats().InUse) // no connections in use.

	t1 := table.NewTableInfo(db, "test", "cutovert1")
	t1new := table.NewTableInfo(db, "test", "_cutovert1_new")
	logger := logrus.New()
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, t1, t1new, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:      logger,
		Concurrency: 4,
		BatchSize:   10000,
	})
	// the feed must be started.
	assert.NoError(t, feed.Run())

	cutover, err := NewCutOver(db, t1, t1new, feed, dbconn.NewDBConfig(), logger)
	assert.NoError(t, err)

	err = cutover.Run(context.Background())
	assert.NoError(t, err)

	assert.Equal(t, 0, db.Stats().InUse) // all connections are returned

	// Verify that t1 has no rows (its lost because we only did cutover, not copy-rows)
	// and t1_old has 2 row.
	// Verify that t2 has one row.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM cutovert1").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
	err = db.QueryRow("SELECT COUNT(*) FROM _cutovert1_old").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestCutOverGhostAlgorithm(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS cutoverghostt1, _cutoverghostt1_new, _cutoverghostt1_old, _cutoverghostt1_chkpnt`)
	tbl := `CREATE TABLE cutoverghostt1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, tbl)
	tbl = `CREATE TABLE _cutoverghostt1_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, tbl)
	runSQL(t, `CREATE TABLE _cutoverghostt1_chkpnt (a int)`) // for binlog advancement

	// The structure is the same, but insert 2 rows in t1 so
	// we can differentiate after the cutover.
	runSQL(t, `INSERT INTO cutoverghostt1 VALUES (1, 2), (2,2)`)

	db, err := dbconn.New(dsn(), dbconn.NewDBConfig())
	assert.NoError(t, err)

	assert.Equal(t, 0, db.Stats().InUse) // no connections in use

	t1 := table.NewTableInfo(db, "test", "cutoverghostt1")
	err = t1.SetInfo(context.Background())
	assert.NoError(t, err)
	t1new := table.NewTableInfo(db, "test", "_cutoverghostt1_new")
	logger := logrus.New()
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, t1, t1new, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:      logger,
		Concurrency: 4,
		BatchSize:   10000,
	})
	// the feed must be started.
	assert.NoError(t, feed.Run())
	// manually assign gh-ost cutover.
	cutover := &CutOver{
		db:        db,
		table:     t1,
		newTable:  t1new,
		feed:      feed,
		dbConfig:  dbconn.NewDBConfig(),
		algorithm: Ghost,
		logger:    logger,
	}
	err = cutover.Run(context.Background())
	assert.NoError(t, err)

	assert.Equal(t, 0, db.Stats().InUse) // all connections are returned

	// Verify that t1 has no rows (its lost because we only did cutover, not copy-rows)
	// and t1_old has 2 row.
	// Verify that t2 has one row.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM cutoverghostt1").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
	err = db.QueryRow("SELECT COUNT(*) FROM _cutoverghostt1_old").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestMDLLockFails(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS mdllocks, _mdllocks_new, _mdllocks_old, _mdllocks_chkpnt`)
	tbl := `CREATE TABLE mdllocks (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, tbl)
	tbl = `CREATE TABLE _mdllocks_new (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, tbl)
	runSQL(t, `CREATE TABLE _mdllocks_chkpnt (a int)`) // for binlog advancement
	// The structure is the same, but insert 2 rows in t1 so
	// we can differentiate after the cutover.
	runSQL(t, `INSERT INTO mdllocks VALUES (1, 2), (2,2)`)

	db, err := dbconn.New(dsn(), dbconn.NewDBConfig())
	assert.NoError(t, err)

	config := dbconn.NewDBConfig()
	config.MaxRetries = 2
	config.LockWaitTimeout = 1

	t1 := table.NewTableInfo(db, "test", "mdllocks")
	t1new := table.NewTableInfo(db, "test", "_mdllocks_new")
	logger := logrus.New()
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, t1, t1new, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:      logger,
		Concurrency: 4,
		BatchSize:   10000,
	})
	// the feed must be started.
	assert.NoError(t, feed.Run())

	cutover, err := NewCutOver(db, t1, t1new, feed, config, logger)
	assert.NoError(t, err)

	// Before we cutover, we READ LOCK the table.
	// This will not fail the table lock but it will fail the rename.
	trx, err := db.BeginTx(context.TODO(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	assert.NoError(t, err)
	_, err = trx.Exec("LOCK TABLES mdllocks READ")
	assert.NoError(t, err)

	// Start the cutover. It will retry in a loop and fail
	// after about 15 seconds (3 sec timeout * 5 retries)
	// or in 5.7 it might fail because it can't find the RENAME in the processlist.
	err = cutover.Run(context.Background())
	assert.Error(t, err)
	assert.NoError(t, trx.Rollback())
}

func TestInvalidOptions(t *testing.T) {
	db, err := dbconn.New(dsn(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	logger := logrus.New()

	// Invalid options
	_, err = NewCutOver(db, nil, nil, nil, dbconn.NewDBConfig(), logger)
	assert.Error(t, err)
	t1 := table.NewTableInfo(db, "test", "t1")
	t1new := table.NewTableInfo(db, "test", "t1_new")
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, t1, t1new, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:      logger,
		Concurrency: 4,
		BatchSize:   10000,
	})
	_, err = NewCutOver(db, nil, t1new, feed, dbconn.NewDBConfig(), logger)
	assert.Error(t, err)
}

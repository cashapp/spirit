package migration

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/squareup/spirit/pkg/repl"
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

func TestCutOver(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS cutovert1, _cutovert1_shadow, _cutovert1_old`)
	tbl := `CREATE TABLE cutovert1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, tbl)
	tbl = `CREATE TABLE _cutovert1_shadow (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, tbl)
	// The structure is the same, but insert 2 rows in t1 so
	// we can differentiate after the cutover.
	runSQL(t, `INSERT INTO cutovert1 VALUES (1, 2), (2,2)`)

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	t1 := table.NewTableInfo("test", "cutovert1")
	t1shadow := table.NewTableInfo("test", "_cutovert1_shadow")
	logger := logrus.New()
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, t1, t1shadow, cfg.User, cfg.Passwd, logger)
	// the feed must be started.
	assert.NoError(t, feed.Run())

	cutover, err := NewCutOver(db, t1, t1shadow, feed, logger)
	assert.NoError(t, err)

	err = cutover.Run(context.Background())
	assert.NoError(t, err)

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

func TestMDLLockFails(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS mdllocks, _mdllocks_shadow, mdllocks_old`)
	tbl := `CREATE TABLE mdllocks (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, tbl)
	tbl = `CREATE TABLE _mdllocks_shadow (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, tbl)
	// The structure is the same, but insert 2 rows in t1 so
	// we can differentiate after the cutover.
	runSQL(t, `INSERT INTO mdllocks VALUES (1, 2), (2,2)`)

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	t1 := table.NewTableInfo("test", "mdllocks")
	t1shadow := table.NewTableInfo("test", "_mdllocks_shadow")
	logger := logrus.New()
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, t1, t1shadow, cfg.User, cfg.Passwd, logger)
	// the feed must be started.
	assert.NoError(t, feed.Run())

	cutover, err := NewCutOver(db, t1, t1shadow, feed, logger)
	assert.NoError(t, err)

	// Before we cutover, we READ LOCK the table.
	// This will not fail the table lock but it will fail the rename.
	trx, err := db.BeginTx(context.TODO(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	assert.NoError(t, err)
	_, err = trx.Exec("LOCK TABLES mdllocks READ")
	assert.NoError(t, err)

	// Start the cutover. It will retry in a loop and fail
	// after about 15 seconds (3 sec timeout * 5 retries)
	err = cutover.Run(context.Background())
	assert.ErrorContains(t, err, "Lock wait timeout exceeded; try restarting transaction")

	assert.NoError(t, trx.Rollback())
}

func TestInvalidOptions(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	logger := logrus.New()

	// Invalid options
	_, err = NewCutOver(db, nil, nil, nil, logger)
	assert.Error(t, err)
	t1 := table.NewTableInfo("test", "t1")
	t1shadow := table.NewTableInfo("test", "t1_shadow")
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	feed := repl.NewClient(db, cfg.Addr, t1, t1shadow, cfg.User, cfg.Passwd, logger)
	_, err = NewCutOver(db, nil, t1shadow, feed, logger)
	assert.Error(t, err)
}

package migration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/spirit/pkg/repl"
	"github.com/squareup/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
)

func dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", TestUser, TestPassword, TestHost, TestSchema)
}

func TestCutOver(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS t1, _t1_shadow, t1_old`)
	tbl := `CREATE TABLE t1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, tbl)
	tbl = `CREATE TABLE _t1_shadow (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, tbl)
	// The structure is the same, but insert 2 rows in t1 so
	// we can differentiate after the cutover.
	runSQL(t, `INSERT INTO t1 VALUES (1, 2), (2,2)`)

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	t1 := table.NewTableInfo("test", "t1")
	t1shadow := table.NewTableInfo("test", "_t1_shadow")
	logger := log.New()
	feed := repl.NewClient(db, TestHost, t1, t1shadow, TestUser, TestPassword, logger)
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
	err = db.QueryRow("SELECT COUNT(*) FROM t1").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
	err = db.QueryRow("SELECT COUNT(*) FROM t1_old").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestInvalidOptions(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	logger := log.New()

	// Invalid options
	_, err = NewCutOver(db, nil, nil, nil, logger)
	assert.Error(t, err)
	t1 := table.NewTableInfo("test", "t1")
	t1shadow := table.NewTableInfo("test", "t1_shadow")
	feed := repl.NewClient(db, TestHost, t1, t1shadow, TestUser, TestPassword, logger)
	_, err = NewCutOver(db, nil, t1shadow, feed, logger)
	assert.Error(t, err)
}

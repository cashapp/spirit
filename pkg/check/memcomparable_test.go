package check

import (
	"context"
	"database/sql"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/squareup/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
)

func TestMemcomparable(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	_, err = db.Exec(`drop table if exists memcomparablecheck_t1, memcomparablecheck_t2`)
	assert.NoError(t, err)
	sql := `CREATE TABLE memcomparablecheck_t1 (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (name)
	);`
	_, err = db.Exec(sql)
	assert.NoError(t, err)
	sql = `CREATE TABLE memcomparablecheck_t2 (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	);`
	_, err = db.Exec(sql)
	assert.NoError(t, err)

	r := Resources{
		DB:    db,
		Table: table.NewTableInfo(db, "test", "memcomparablecheck_t1"),
	}
	assert.NoError(t, r.Table.SetInfo(context.Background()))
	err = memcomparableCheck(context.Background(), r, logrus.New())
	assert.Error(t, err) // not good.

	r.Table = table.NewTableInfo(db, "test", "memcomparablecheck_t2")
	assert.NoError(t, r.Table.SetInfo(context.Background()))
	err = memcomparableCheck(context.Background(), r, logrus.New())
	assert.NoError(t, err) // noerror
}

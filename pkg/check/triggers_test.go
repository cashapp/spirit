package check

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/pingcap/tidb/parser/test_driver"
	"github.com/sirupsen/logrus"
	"github.com/squareup/spirit/pkg/table"

	"github.com/stretchr/testify/assert"
)

func TestAddTriggers(t *testing.T) {
	r := Resources{
		Table: &table.TableInfo{TableName: "account"},
		Alter: "CREATE TRIGGER ins_sum BEFORE INSERT ON account FOR EACH ROW SET @sum = @sum + NEW.amount;",
	}
	err := addTriggersCheck(context.Background(), r, logrus.New())
	assert.Error(t, err) // add triggers
	assert.ErrorContains(t, err, "adding triggers is not supported")

	r.Alter = "DROP COLUMN foo"
	err = addForeignKeyCheck(context.Background(), r, logrus.New())
	assert.NoError(t, err) // regular DDL

	r.Alter = "bogus"
	err = addForeignKeyCheck(context.Background(), r, logrus.New())
	assert.Error(t, err) // not a valid ddl
}

func TestHasTriggers(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	_, err = db.Exec(`drop table if exists account`)
	assert.NoError(t, err)
	_, err = db.Exec(`drop trigger if exists ins_sum`)
	assert.NoError(t, err)
	sql := `CREATE TABLE account (
		acct_num INT,
		amount DECIMAL (10,2),
		PRIMARY KEY (acct_num)
	);`
	_, err = db.Exec(sql)
	assert.NoError(t, err)
	sql = `CREATE TRIGGER ins_sum BEFORE INSERT ON account
		FOR EACH ROW SET @sum = @sum + NEW.amount;`
	_, err = db.Exec(sql)
	assert.NoError(t, err)

	r := Resources{
		DB:    db,
		Table: &table.TableInfo{SchemaName: "test", TableName: "account"},
		Alter: "Engine=innodb",
	}

	err = hasTriggersCheck(context.Background(), r, logrus.New())
	assert.ErrorContains(t, err, "") // already has a trigger associated.

	_, err = db.Exec(`drop trigger if exists ins_sum`)
	assert.NoError(t, err)
	err = hasTriggersCheck(context.Background(), r, logrus.New())
	assert.NoError(t, err) // no longer said to have trigger associated.
}

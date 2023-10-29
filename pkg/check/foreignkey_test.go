package check

import (
	"context"
	"database/sql"
	"testing"

	"github.com/cashapp/spirit/pkg/table"
	"github.com/cashapp/spirit/pkg/testutils"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestAddForeignKey(t *testing.T) {
	r := Resources{
		Table: &table.TableInfo{TableName: "test"},
		Alter: "ADD FOREIGN KEY (customer_id) REFERENCES customers (id)",
	}
	err := addForeignKeyCheck(context.Background(), r, logrus.New())
	assert.Error(t, err) // add foreign key
	assert.ErrorContains(t, err, "adding foreign key constraints is not supported")

	r.Alter = "DROP COLUMN foo"
	err = addForeignKeyCheck(context.Background(), r, logrus.New())
	assert.NoError(t, err) // regular DDL

	r.Alter = "bogus"
	err = addForeignKeyCheck(context.Background(), r, logrus.New())
	assert.Error(t, err) // not a valid ddl
}

func TestHasForeignKey(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)

	_, err = db.Exec(`drop table if exists customers, customer_contacts`)
	assert.NoError(t, err)
	sql := `CREATE TABLE customers (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	);`
	_, err = db.Exec(sql)
	assert.NoError(t, err)
	sql = `CREATE TABLE customer_contacts (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		customer_id INT NOT NULL,
		PRIMARY KEY (id),
		INDEX  (customer_id),  
		CONSTRAINT fk_customer FOREIGN KEY (customer_id)  
		REFERENCES customers(id)  
		ON DELETE CASCADE  
		ON UPDATE CASCADE  
	);`
	_, err = db.Exec(sql)
	assert.NoError(t, err)

	// Under this model, both customers and customer_contacts are said to have foreign keys.
	r := Resources{
		DB:    db,
		Table: &table.TableInfo{SchemaName: "test", TableName: "customers"},
		Alter: "Engine=innodb",
	}
	err = hasForeignKeysCheck(context.Background(), r, logrus.New())
	assert.Error(t, err) // already has foreign keys.

	r.Table.TableName = "customer_contacts"
	err = hasForeignKeysCheck(context.Background(), r, logrus.New())
	assert.Error(t, err) // already has foreign keys.

	_, err = db.Exec(`drop table if exists customer_contacts`)
	assert.NoError(t, err)
	r.Table.TableName = "customers"
	err = hasForeignKeysCheck(context.Background(), r, logrus.New())
	assert.NoError(t, err) // no longer said to have foreign keys.
}

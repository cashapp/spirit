package dbconn

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/squareup/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
)

func TestTableLock(t *testing.T) {
	db, err := New(dsn())
	assert.NoError(t, err)
	defer db.Close()
	config := NewDBConfig()
	config.LockWaitTimeout = 2
	pool, err := NewConnPool(context.TODO(), db, 2, config, logrus.New())
	assert.NoError(t, err)

	err = pool.Exec(context.Background(), "DROP TABLE IF EXISTS test.testlock")
	assert.NoError(t, err)
	err = pool.Exec(context.Background(), "CREATE TABLE test.testlock (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)

	tbl := &table.TableInfo{SchemaName: "test", TableName: "testlock", QuotedName: "`test`.`testlock`"}

	lock1, err := pool.NewTableLock(context.Background(), tbl, false)
	assert.NoError(t, err)

	// Try to acquire a table that is already locked, should pass *because*
	// table locks are actually READ locks. We do this so copies can still occur,
	// only writes are blocked.
	lock2, err := pool.NewTableLock(context.Background(), tbl, false)
	assert.NoError(t, err)

	assert.NoError(t, lock1.Close())
	assert.NoError(t, lock2.Close())
}

func TestTableLockFail(t *testing.T) {
	db, err := New(dsn())
	assert.NoError(t, err)
	defer db.Close()

	config := NewDBConfig()
	config.MaxRetries = 1
	config.LockWaitTimeout = 1
	pool, err := NewConnPool(context.TODO(), db, 2, config, logrus.New())
	assert.NoError(t, err)
	defer pool.Close()

	err = pool.Exec(context.Background(), "DROP TABLE IF EXISTS test.testlockfail")
	assert.NoError(t, err)
	err = pool.Exec(context.Background(), "CREATE TABLE test.testlockfail (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)

	// We acquire an exclusive lock first, so the tablelock should fail.
	trx, err := db.Begin()
	assert.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_, err = trx.Exec("LOCK TABLES test.testlockfail WRITE")
		assert.NoError(t, err)
		wg.Done()
		time.Sleep(5 * time.Second)
		err = trx.Rollback()
		assert.NoError(t, err)
	}()
	wg.Wait()

	tbl := &table.TableInfo{SchemaName: "test", TableName: "testlockfail"}
	_, err = pool.NewTableLock(context.Background(), tbl, false)
	assert.Error(t, err)
}

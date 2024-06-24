package dbconn

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cashapp/spirit/pkg/table"
	"github.com/cashapp/spirit/pkg/testutils"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func testConfig() *DBConfig {
	config := NewDBConfig()
	config.LockWaitTimeout = 1
	return config
}

func TestTableLock(t *testing.T) {
	db, err := New(testutils.DSN(), testConfig())
	assert.NoError(t, err)
	defer db.Close()
	err = Exec(context.Background(), db, "DROP TABLE IF EXISTS test.testlock")
	assert.NoError(t, err)
	err = Exec(context.Background(), db, "CREATE TABLE test.testlock (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)

	tbl := &table.TableInfo{SchemaName: "test", TableName: "testlock", QuotedName: "`test`.`testlock`"}

	lock1, err := NewTableLock(context.Background(), db, tbl, testConfig(), logrus.New())
	assert.NoError(t, err)

	// Try to acquire a table that is already locked, should fail because we use WRITE locks now.
	// But should also fail very quickly because we've set the lock_wait_timeout to 1s.
	_, err = NewTableLock(context.Background(), db, tbl, testConfig(), logrus.New())
	assert.Error(t, err)

	assert.NoError(t, lock1.Close())
}

func TestExecUnderLock(t *testing.T) {
	db, err := New(testutils.DSN(), testConfig())
	assert.NoError(t, err)
	defer db.Close()
	err = Exec(context.Background(), db, "DROP TABLE IF EXISTS testunderlock, _testunderlock_new")
	assert.NoError(t, err)
	err = Exec(context.Background(), db, "CREATE TABLE testunderlock (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)
	err = Exec(context.Background(), db, "CREATE TABLE _testunderlock_new (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)

	tbl := &table.TableInfo{SchemaName: "test", TableName: "testunderlock", QuotedName: "`test`.`testunderlock`"}
	lock, err := NewTableLock(context.Background(), db, tbl, testConfig(), logrus.New())
	assert.NoError(t, err)
	err = lock.ExecUnderLock(context.Background(), []string{"INSERT INTO testunderlock VALUES (1, 1)", "", "INSERT INTO testunderlock VALUES (2, 2)"})
	assert.NoError(t, err) // pass, under write lock.

	// Try to execute a statement that is not in the lock transaction though
	// It is expected to fail.
	err = Exec(context.Background(), db, "INSERT INTO testunderlock VALUES (3, 3)")
	assert.Error(t, err)
}

func TestTableLockFail(t *testing.T) {
	db, err := New(testutils.DSN(), testConfig())
	assert.NoError(t, err)
	defer db.Close()

	err = Exec(context.Background(), db, "DROP TABLE IF EXISTS test.testlockfail")
	assert.NoError(t, err)
	err = Exec(context.Background(), db, "CREATE TABLE test.testlockfail (id INT NOT NULL PRIMARY KEY, colb int)")
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
	_, err = NewTableLock(context.Background(), db, tbl, testConfig(), logrus.New())
	assert.Error(t, err)
}

package dbconn

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestRetryableTrx(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()
	config := NewDBConfig()

	pool, err := NewConnPool(context.TODO(), db, 2, NewDBConfig(), logrus.New())
	assert.NoError(t, err)
	defer pool.Close()

	err = pool.Exec(context.Background(), "DROP TABLE IF EXISTS test.dbexec")
	assert.NoError(t, err)
	err = pool.Exec(context.Background(), "CREATE TABLE test.dbexec (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)

	stmts := []string{
		"INSERT INTO test.dbexec (id, colb) VALUES (1, 1)",
		"", // test empty
		"INSERT INTO test.dbexec (id, colb) VALUES (2, 2)",
	}
	_, err = pool.RetryableTransaction(context.Background(), true, stmts...)
	assert.NoError(t, err)

	_, err = pool.RetryableTransaction(context.Background(), true, "INSERT INTO test.dbexec (id, colb) VALUES (2, 2)") // duplicate
	assert.Error(t, err)

	// duplicate, but creates a warning. Ignore duplicate warnings set to true.
	_, err = pool.RetryableTransaction(context.Background(), true, "INSERT IGNORE INTO test.dbexec (id, colb) VALUES (2, 2)")
	assert.NoError(t, err)

	// duplicate, but warning not ignored
	_, err = pool.RetryableTransaction(context.Background(), false, "INSERT IGNORE INTO test.dbexec (id, colb) VALUES (2, 2)")
	assert.Error(t, err)

	// start a transaction, acquire a lock for long enough that the first attempt times out
	// but a retry is successful.
	config.InnodbLockWaitTimeout = 1
	trx, err := db.Begin()
	assert.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_, err = trx.Exec("SELECT * FROM test.dbexec WHERE id = 1 FOR UPDATE")
		assert.NoError(t, err)
		wg.Done()
		time.Sleep(2 * time.Second)
		err = trx.Rollback()
		assert.NoError(t, err)
	}()
	wg.Wait()
	_, err = pool.RetryableTransaction(context.Background(), false, "UPDATE test.dbexec SET colb=123 WHERE id = 1")
	assert.NoError(t, err)

	// Same again, but make the retry unsuccessful
	config.InnodbLockWaitTimeout = 1
	config.MaxRetries = 2
	trx, err = db.Begin()
	assert.NoError(t, err)
	wg.Add(1)
	go func() {
		_, err = trx.Exec("SELECT * FROM test.dbexec WHERE id = 2 FOR UPDATE")
		assert.NoError(t, err)
		wg.Done()
	}()
	wg.Wait()
	_, err = pool.RetryableTransaction(context.Background(), false, "UPDATE test.dbexec SET colb=123 WHERE id = 2") // this will fail, since it times out and exhausts retries.
	assert.Error(t, err)
	err = trx.Rollback() // now we can rollback.
	assert.NoError(t, err)
}

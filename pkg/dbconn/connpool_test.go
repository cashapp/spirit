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

func TestConnPool_Get_Put(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	config := NewDBConfig()
	config.MaxRetries = 1

	pool, err := NewConnPool(context.TODO(), db, 2, config, logrus.New())
	assert.NoError(t, err)
	defer pool.Close()

	// Pull both connections from pool
	conn1, err := pool.Get(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, conn1)
	conn2, err := pool.Get(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, conn2)

	// Put back one and get to check if it's the same connection
	pool.Put(context.Background(), conn1)
	conn3, err := pool.Get(context.Background())
	assert.Equal(t, conn1, conn3)
	assert.NoError(t, err)
}

func TestConnPool_Close(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	pool, err := NewConnPool(context.TODO(), db, 4, NewDBConfig(), logrus.New())
	assert.NoError(t, err)

	// Test that closing the pool sets the conns to nil
	assert.NoError(t, pool.Close())
	assert.Nil(t, pool.conns)
}

func TestConnPool_GetWithConnectionID(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	pool, err := NewConnPool(context.TODO(), db, 4, NewDBConfig(), logrus.New())
	assert.NoError(t, err)
	defer pool.Close()

	conn, id, err := pool.GetWithConnectionID(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, conn)

	var connID int
	err = conn.QueryRowContext(context.Background(), "SELECT CONNECTION_ID()").Scan(&connID)
	assert.NoError(t, err)
	assert.Equal(t, connID, id)
}

func TestConnPool_Size(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	pool, err := NewConnPool(context.TODO(), db, 4, NewDBConfig(), logrus.New())
	assert.NoError(t, err)
	defer pool.Close()

	// Check size after initialization
	assert.Equal(t, 4, pool.Size())

	_, err = pool.Get(context.Background())
	assert.NoError(t, err)
	_, err = pool.Get(context.Background())
	assert.NoError(t, err)

	// Check size after pulling out some connections
	assert.Equal(t, 2, pool.Size())
}
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

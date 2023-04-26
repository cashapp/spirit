package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func dsn() string {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return "msandbox:msandbox@tcp(127.0.0.1:8030)/test"
	}
	return dsn
}

func getVariable(trx *sql.Tx, name string, sessionScope bool) (string, error) {
	var value string
	scope := "GLOBAL"
	if sessionScope {
		scope = "SESSION"
	}
	err := trx.QueryRow("SELECT @@" + scope + "." + name).Scan(&value)
	return value, err
}

func TestLockWaitTimeouts(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	trx, err := db.Begin()
	assert.NoError(t, err)

	globalLockWaitTimeout, err := getVariable(trx, "lock_wait_timeout", false)
	assert.NoError(t, err)
	globalInnodbLockWaitTimeout, err := getVariable(trx, "innodb_lock_wait_timeout", false)
	assert.NoError(t, err)

	// Check lock wait timeout and innodb lock wait timeout
	// match the global values.
	mysqlVar, err := getVariable(trx, "lock_wait_timeout", true)
	assert.NoError(t, err)
	assert.Equal(t, globalLockWaitTimeout, mysqlVar)
	mysqlVar, err = getVariable(trx, "innodb_lock_wait_timeout", true)
	assert.NoError(t, err)
	assert.Equal(t, globalInnodbLockWaitTimeout, mysqlVar)

	err = standardizeTrx(context.Background(), trx, 0)
	assert.NoError(t, err)

	// Check the timeouts are shorter
	mysqlVar, err = getVariable(trx, "lock_wait_timeout", true)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprint(mdlLockWaitTimeout), mysqlVar)
	mysqlVar, err = getVariable(trx, "innodb_lock_wait_timeout", true)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprint(innodbLockWaitTimeout), mysqlVar)

	// But if we extend the retry number, they are longer.
	err = standardizeTrx(context.Background(), trx, 2)
	assert.NoError(t, err)
	mysqlVar, err = getVariable(trx, "lock_wait_timeout", true)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprint(mdlLockWaitTimeout+2), mysqlVar)
	mysqlVar, err = getVariable(trx, "innodb_lock_wait_timeout", true)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprint(innodbLockWaitTimeout+2), mysqlVar)
}

func TestRetryableTrx(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	err = DBExec(context.Background(), db, "DROP TABLE IF EXISTS test.dbexec")
	assert.NoError(t, err)
	err = DBExec(context.Background(), db, "CREATE TABLE test.dbexec (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)

	stmts := []string{
		"INSERT INTO test.dbexec (id, colb) VALUES (1, 1)",
		"", // test empty
		"INSERT INTO test.dbexec (id, colb) VALUES (2, 2)",
	}
	_, err = RetryableTransaction(context.Background(), db, true, stmts...)
	assert.NoError(t, err)

	_, err = RetryableTransaction(context.Background(), db, true, "INSERT INTO test.dbexec (id, colb) VALUES (2, 2)") // duplicate
	assert.Error(t, err)

	// duplicate, but creates a warning. Ignore duplicate warnings set to true.
	_, err = RetryableTransaction(context.Background(), db, true, "INSERT IGNORE INTO test.dbexec (id, colb) VALUES (2, 2)")
	assert.NoError(t, err)

	// duplicate, but warning not ignored
	_, err = RetryableTransaction(context.Background(), db, false, "INSERT IGNORE INTO test.dbexec (id, colb) VALUES (2, 2)")
	assert.Error(t, err)

	// start a transaction, acquire a lock for long enough that the first attempt times out
	// but a retry is successful.
	innodbLockWaitTimeout = 1
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
	_, err = RetryableTransaction(context.Background(), db, false, "UPDATE test.dbexec SET colb=123 WHERE id = 1")
	assert.NoError(t, err)

	// Same again, but make the retry unsuccessful
	innodbLockWaitTimeout = 1
	maxRetries = 2
	trx, err = db.Begin()
	assert.NoError(t, err)
	wg.Add(1)
	go func() {
		_, err = trx.Exec("SELECT * FROM test.dbexec WHERE id = 2 FOR UPDATE")
		assert.NoError(t, err)
		wg.Done()
		time.Sleep(4 * time.Second)
		err = trx.Rollback()
		assert.NoError(t, err)
	}()
	wg.Wait()
	_, err = RetryableTransaction(context.Background(), db, false, "UPDATE test.dbexec SET colb=123 WHERE id = 2")
	assert.Error(t, err)
}

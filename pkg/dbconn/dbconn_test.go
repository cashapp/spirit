package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"os"
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

	config := NewDBConfig()
	config.LockWaitTimeout = 10

	err = standardizeTrx(context.Background(), trx, config)
	assert.NoError(t, err)

	// Check the timeouts are shorter
	mysqlVar, err = getVariable(trx, "lock_wait_timeout", true)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprint(config.LockWaitTimeout), mysqlVar)
	mysqlVar, err = getVariable(trx, "innodb_lock_wait_timeout", true)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprint(config.InnodbLockWaitTimeout), mysqlVar)
}

func TestRetryableTrx(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()
	config := NewDBConfig()
	err = DBExec(context.Background(), db, config, "DROP TABLE IF EXISTS test.dbexec")
	assert.NoError(t, err)
	err = DBExec(context.Background(), db, config, "CREATE TABLE test.dbexec (id INT NOT NULL PRIMARY KEY, colb int)")
	assert.NoError(t, err)

	stmts := []string{
		"INSERT INTO test.dbexec (id, colb) VALUES (1, 1)",
		"", // test empty
		"INSERT INTO test.dbexec (id, colb) VALUES (2, 2)",
	}
	_, err = RetryableTransaction(context.Background(), db, true, NewDBConfig(), stmts...)
	assert.NoError(t, err)

	_, err = RetryableTransaction(context.Background(), db, true, NewDBConfig(), "INSERT INTO test.dbexec (id, colb) VALUES (2, 2)") // duplicate
	assert.Error(t, err)

	// duplicate, but creates a warning. Ignore duplicate warnings set to true.
	_, err = RetryableTransaction(context.Background(), db, true, NewDBConfig(), "INSERT IGNORE INTO test.dbexec (id, colb) VALUES (2, 2)")
	assert.NoError(t, err)

	// duplicate, but warning not ignored
	_, err = RetryableTransaction(context.Background(), db, false, NewDBConfig(), "INSERT IGNORE INTO test.dbexec (id, colb) VALUES (2, 2)")
	assert.Error(t, err)

	// start a transaction, acquire a lock for long enough that the first attempt times out
	// but a retry is successful.
	config.InnodbLockWaitTimeout = 1
	trx, err := db.Begin()
	assert.NoError(t, err)
	_, err = trx.Exec("SELECT * FROM test.dbexec WHERE id = 1 FOR UPDATE")
	assert.NoError(t, err)
	go func() {
		time.Sleep(2 * time.Second)
		err2 := trx.Rollback()
		assert.NoError(t, err2)
	}()
	_, err = RetryableTransaction(context.Background(), db, false, config, "UPDATE test.dbexec SET colb=123 WHERE id = 1")
	assert.NoError(t, err)

	// Same again, but make the retry unsuccessful
	config.InnodbLockWaitTimeout = 1
	config.MaxRetries = 2
	trx, err = db.Begin()
	assert.NoError(t, err)
	_, err = trx.Exec("SELECT * FROM test.dbexec WHERE id = 2 FOR UPDATE")
	assert.NoError(t, err)
	_, err = RetryableTransaction(context.Background(), db, false, config, "UPDATE test.dbexec SET colb=123 WHERE id = 2") // this will fail, since it times out and exhausts retries.
	assert.Error(t, err)
	err = trx.Rollback() // now we can rollback.
	assert.NoError(t, err)
}

package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
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

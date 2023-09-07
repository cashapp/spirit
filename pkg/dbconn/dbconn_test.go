package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func dsn() string {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return "msandbox:msandbox@tcp(127.0.0.1:8030)/test"
	}
	return dsn
}

func getVariable(conn *sql.Conn, name string, sessionScope bool) (string, error) {
	var value string
	scope := "GLOBAL"
	if sessionScope {
		scope = "SESSION"
	}
	err := conn.QueryRowContext(context.Background(), "SELECT @@"+scope+"."+name).Scan(&value)
	return value, err
}

func TestLockWaitTimeouts(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	// Use non standard generic connection
	conn, err := db.Conn(context.Background())
	assert.NoError(t, err)

	globalLockWaitTimeout, err := getVariable(conn, "lock_wait_timeout", false)
	assert.NoError(t, err)
	globalInnodbLockWaitTimeout, err := getVariable(conn, "innodb_lock_wait_timeout", false)
	assert.NoError(t, err)

	// Check lock wait timeout and innodb lock wait timeout
	// match the global values.
	mysqlVar, err := getVariable(conn, "lock_wait_timeout", true)
	assert.NoError(t, err)
	assert.Equal(t, globalLockWaitTimeout, mysqlVar)
	mysqlVar, err = getVariable(conn, "innodb_lock_wait_timeout", true)
	assert.NoError(t, err)
	assert.Equal(t, globalInnodbLockWaitTimeout, mysqlVar)

	config := NewDBConfig()
	config.LockWaitTimeout = 10
	pool, err := NewConnPool(context.Background(), db, 1, config)
	assert.NoError(t, err)

	conn, err = pool.Get()
	assert.NoError(t, err)

	// Check the timeouts are shorter when using connection from custom pool
	mysqlVar, err = getVariable(conn, "lock_wait_timeout", true)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprint(config.LockWaitTimeout), mysqlVar)
	mysqlVar, err = getVariable(conn, "innodb_lock_wait_timeout", true)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprint(config.InnodbLockWaitTimeout), mysqlVar)
}

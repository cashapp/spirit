// Package dbconn contains a series of database-related utility functions.
package dbconn

import (
	"context"
	"database/sql"
	"math/rand"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	errLockWaitTimeout = 1205
	errDeadlock        = 1213
	errCannotConnect   = 2003
	errConnLost        = 2013
	errReadOnly        = 1290
	errQueryKilled     = 1836
)

type DBConfig struct {
	LockWaitTimeout       int
	InnodbLockWaitTimeout int
	MaxRetries            int
}

func NewDBConfig() *DBConfig {
	return &DBConfig{
		LockWaitTimeout:       30,
		InnodbLockWaitTimeout: 3,
		MaxRetries:            5,
	}
}

func standardizeConn(ctx context.Context, conn *sql.Conn, config *DBConfig) error {
	_, err := conn.ExecContext(ctx, "SET time_zone='+00:00'")
	if err != nil {
		return err
	}
	// This looks ill-advised, but unfortunately it's required.
	// A user might have set their SQL mode to empty even if the
	// server has it enabled. After they've inserted data,
	// we need to be able to produce the same when copying.
	// If you look at standard packages like wordpress, drupal etc.
	// they all change the SQL mode. If you look at mysqldump, etc.
	// they all unset the SQL mode just like this.
	_, err = conn.ExecContext(ctx, "SET sql_mode=''")
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "SET NAMES 'binary'")
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "SET innodb_lock_wait_timeout=?", config.InnodbLockWaitTimeout)
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "SET lock_wait_timeout=?", config.LockWaitTimeout)
	if err != nil {
		return err
	}
	return nil
}

// canRetryError looks at the MySQL error and decides if it is considered
// a permanent failure or not. For simplicity a "retryable" error means
// rollback the transaction and start the transaction again.
// This is because it gets complicated in cases where the statement could
// succeed but then there is a deadlock later on.
func canRetryError(err error) bool {
	var errNumber uint16
	if val, ok := err.(*mysql.MySQLError); ok {
		errNumber = val.Number
	}
	switch errNumber {
	case errLockWaitTimeout, errDeadlock, errCannotConnect,
		errConnLost, errReadOnly, errQueryKilled:
		return true
	default:
		return false
	}
}

// backoff sleeps a few milliseconds before retrying.
func backoff(i int) {
	randFactor := i * rand.Intn(10) * int(time.Millisecond)
	time.Sleep(time.Duration(randFactor))
}

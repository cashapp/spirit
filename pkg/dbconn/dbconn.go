// Package dbconn contains a series of database-related utility functions.
package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	my "github.com/go-mysql/errors"
	"github.com/squareup/spirit/pkg/utils"
)

const (
	maxRetries            = 5
	errLockWaitTimeout    = 1205
	errDeadlock           = 1213
	innodbLockWaitTimeout = 1 // usually fine, since it's a lock on a row.
	mdlLockWaitTimeout    = 3 // safer to make slightly longer.
)

func standardizeTrx(trx *sql.Tx) error {
	_, err := trx.Exec("SET time_zone='+00:00'")
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
	_, err = trx.Exec("SET sql_mode=''")
	if err != nil {
		return err
	}
	_, err = trx.Exec("SET NAMES 'binary'")
	if err != nil {
		return err
	}
	_, err = trx.Exec("SET innodb_lock_wait_timeout=?", innodbLockWaitTimeout)
	if err != nil {
		return err
	}
	return nil
}

// RetryableTransaction retries all statements in a transaction, retrying if a statement
// errors, or there is a deadlock. It will retry up to maxRetries times.
func RetryableTransaction(ctx context.Context, db *sql.DB, ignoreDupKeyWarnings bool, stmts ...string) (int64, error) {
	var err error
	var trx *sql.Tx
	var rowsAffected int64
RETRYLOOP:
	for i := 0; i < maxRetries; i++ {
		// Start a transaction
		if trx, err = db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err != nil {
			backoff(i)
			continue RETRYLOOP // retry
		}
		// Standardize it.
		if err = standardizeTrx(trx); err != nil {
			utils.ErrInErr(trx.Rollback()) // Rollback
			backoff(i)
			continue RETRYLOOP // retry
		}
		// Execute all statements.
		for _, stmt := range stmts {
			if stmt == "" {
				continue
			}
			// For simplicity a "retryable" error means rollback the transaction and start the transaction again,
			// not just retry the statement. This is because it gets complicated in cases where the statement could
			// succeed but then there is a deadlock later on. The myerror library also doesn't differentiate between
			// retryable (new transaction) and retryable (same transaction)
			var res sql.Result
			if res, err = trx.Exec(stmt); err != nil {
				_, myerr := my.Error(err)
				if my.CanRetry(myerr) || my.MySQLErrorCode(err) == errLockWaitTimeout || my.MySQLErrorCode(err) == errDeadlock {
					utils.ErrInErr(trx.Rollback()) // Rollback
					backoff(i)
					continue RETRYLOOP // retry
				}
				utils.ErrInErr(trx.Rollback()) // Rollback
				return rowsAffected, err
			}
			// Even though there was no ERROR we still need to inspect SHOW WARNINGS
			// This is because many of the statements use INSERT IGNORE.
			warningRes, err := trx.Query("SHOW WARNINGS") //nolint: execinquery
			if err != nil {
				utils.ErrInErr(trx.Rollback()) // Rollback
				return rowsAffected, err
			}
			var level, code, message string
			for warningRes.Next() {
				err = warningRes.Scan(&level, &code, &message)
				if err != nil {
					utils.ErrInErr(trx.Rollback()) // Rollback
					return rowsAffected, err
				}
				// We won't receive out of range warnings (1264)
				// because the SQL mode has been unset. This is important
				// because a historical value like 0000-00-00 00:00:00
				// might exist in the table and needs to be copied.
				if code == "1062" && ignoreDupKeyWarnings {
					continue // ignore duplicate key warnings
				} else {
					utils.ErrInErr(trx.Rollback())
					return rowsAffected, fmt.Errorf("unsafe warning migrating chunk: %s", message)
				}
			}
			// As long as it is a statement that supports affected rows (err == nil)
			// Get the number of rows affected and add it to the total balance.
			count, err := res.RowsAffected()
			if err == nil { // supported
				rowsAffected += count
			}
		}
		if err != nil {
			utils.ErrInErr(trx.Rollback()) // Rollback
			backoff(i)
			continue RETRYLOOP
		}
		// Commit it.
		if err = trx.Commit(); err != nil {
			utils.ErrInErr(trx.Rollback())
			backoff(i)
			continue RETRYLOOP
		}
		// Success!
		return rowsAffected, nil
	}
	// We failed too many times, return the last error
	return rowsAffected, err
}

// backoff sleeps a few milliseconds before retrying.
func backoff(i int) {
	randFactor := i * rand.Intn(10) * int(time.Millisecond)
	time.Sleep(time.Duration(randFactor))
}

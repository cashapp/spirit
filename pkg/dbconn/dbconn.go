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
	errLockWaitTimeout = 1205
	errDeadlock        = 1213
)

var (
	innodbLockWaitTimeout = 3   // usually fine, since it's a lock on a row.
	mdlLockWaitTimeout    = 20  // safer to make slightly longer.
	MaxRetries            = 10  // each retry extends both lock timeouts by 1 second.
	maximumLockTime       = 120 // safety measure incase we make changes: don't allow any lock longer than this
)

func standardizeTrx(ctx context.Context, trx *sql.Tx, retryNumber int) error {
	_, err := trx.ExecContext(ctx, "SET time_zone='+00:00'")
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
	_, err = trx.ExecContext(ctx, "SET sql_mode=''")
	if err != nil {
		return err
	}
	_, err = trx.ExecContext(ctx, "SET NAMES 'binary'")
	if err != nil {
		return err
	}
	_, err = trx.ExecContext(ctx, "SET innodb_lock_wait_timeout=?", utils.BoundaryCheck(innodbLockWaitTimeout+retryNumber, maximumLockTime))
	if err != nil {
		return err
	}
	_, err = trx.ExecContext(ctx, "SET lock_wait_timeout=?", utils.BoundaryCheck(mdlLockWaitTimeout+retryNumber, maximumLockTime))
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
	for i := 0; i < MaxRetries; i++ {
		// Start a transaction
		if trx, err = db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err != nil {
			backoff(i)
			continue RETRYLOOP // retry
		}
		// Standardize it.
		if err = standardizeTrx(ctx, trx, i); err != nil {
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
			if res, err = trx.ExecContext(ctx, stmt); err != nil {
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
			warningRes, err := trx.QueryContext(ctx, "SHOW WARNINGS") //nolint: execinquery
			if err != nil {
				utils.ErrInErr(trx.Rollback()) // Rollback
				return rowsAffected, err
			}
			defer warningRes.Close()
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
				} else if code == "3170" {
					// ER_CAPACITY_EXCEEDED
					// "Memory capacity of 8388608 bytes for 'range_optimizer_max_mem_size' exceeded.
					// Range optimization was not done for this query."
					// i.e. the query still executes it just doesn't optimize perfectly
					continue
				} else {
					utils.ErrInErr(trx.Rollback())
					return rowsAffected, fmt.Errorf("unsafe warning migrating chunk: %s, query: %s", message, stmt)
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

// DBExec is like db.Exec but sets the lock timeout to low in advance.
// Does not require retry, or return a result.
func DBExec(ctx context.Context, db *sql.DB, query string) error {
	trx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	if err := standardizeTrx(ctx, trx, 0); err != nil {
		return err
	}
	_, err = trx.ExecContext(ctx, query)
	return err
}

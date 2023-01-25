// Package dbconn contains a series of database-related utility functions.
package dbconn

import (
	"database/sql"
	"time"

	my "github.com/go-mysql/errors"
)

const maxRetries = 5

func StandardizeTrx(trx *sql.Tx) error {
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
	_, err = trx.Exec("SET innodb_lock_wait_timeout=1")
	if err != nil {
		return err
	}
	return nil
}

func TrxExecWithRetry(trx *sql.Tx, stmt string) (res sql.Result, err error) {
	for i := 0; i < maxRetries; i++ {
		res, err = trx.Exec(stmt)
		if err != nil {
			_, myerr := my.Error(err)
			if my.CanRetry(myerr) || my.MySQLErrorCode(err) == 1205 {
				// Sleep for a bit and retry.
				time.Sleep(100 * time.Millisecond)
				continue
			}
			break // error is non-retryable
		}
		break // err is nil
	}
	return res, err
}

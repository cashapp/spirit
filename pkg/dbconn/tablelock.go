package dbconn

import (
	"context"
	"database/sql"

	"github.com/siddontang/loggers"

	my "github.com/go-mysql/errors"

	"github.com/squareup/spirit/pkg/table"
	"github.com/squareup/spirit/pkg/utils"
)

type TableLock struct {
	table   *table.TableInfo
	lockTxn *sql.Tx
	logger  loggers.Advanced
}

// NewTableLock creates a new server wide lock on a table.
// i.e. LOCK TABLES .. READ.
// It uses a short-timeout with backoff and retry, since if there is a long-running
// process that currently prevents the lock by being acquired, it is considered "nice"
// to let a few short-running processes slip in and proceed, then optimistically try
// and acquire the lock again.
func NewTableLock(ctx context.Context, db *sql.DB, table *table.TableInfo, logger loggers.Advanced) (*TableLock, error) {
	lockTxn, _ := db.BeginTx(ctx, nil)
	_, err := lockTxn.ExecContext(ctx, "SET SESSION lock_wait_timeout = ?", mdlLockWaitTimeout)
	if err != nil {
		return nil, err // could not change timeout.
	}
	for i := 0; i < maxRetries; i++ {
		// In gh-ost they lock the _old table name as well.
		// this might prevent a weird case that we don't handle yet.
		// instead, we DROP IF EXISTS just before the rename, which
		// has a brief race.
		logger.Warnf("trying to acquire table lock, timeout: %d", mdlLockWaitTimeout)
		// TODO: We acquire a READ LOCK which I believe is sufficient (just need to prevent modifications to table).
		// Ghost however, acquires a WRITE LOCK. We can't do that because the slowly arriving
		// changes in BlockWait() will block because they won't be able to read the source table.
		_, err = lockTxn.ExecContext(ctx, "LOCK TABLES "+table.QuotedName+" READ")
		if err != nil {
			// See if the error is retryable, many are
			_, myerr := my.Error(err)
			if my.CanRetry(myerr) || my.MySQLErrorCode(err) == errLockWaitTimeout {
				logger.Warnf("failed trying to acquire table lock, backing off and retrying: %v", err)
				backoff(i)
				continue
			}
			// else not retryable
			return nil, err
		}
		// else success!
		logger.Warn("table lock acquired")
		return &TableLock{
			table:   table,
			lockTxn: lockTxn,
			logger:  logger,
		}, nil
	}
	// The loop ended without success.
	// Return the last error
	utils.ErrInErr(lockTxn.Rollback())
	return nil, err
}

// Close closes the table lock
func (s *TableLock) Close() error {
	_, err := s.lockTxn.Exec("UNLOCK TABLES")
	if err != nil {
		return err
	}
	err = s.lockTxn.Rollback()
	if err != nil {
		return err
	}
	s.logger.Warn("table lock released")
	return nil
}

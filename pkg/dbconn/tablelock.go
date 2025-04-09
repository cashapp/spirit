package dbconn

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/siddontang/loggers"

	"github.com/cashapp/spirit/pkg/table"
)

type TableLock struct {
	table   *table.TableInfo
	lockTxn *sql.Tx
	logger  loggers.Advanced
}

// NewTableLock creates a new server wide lock on a table.
// i.e. LOCK TABLES .. WRITE.
// It uses a short-timeout with backoff and retry, since if there is a long-running
// process that currently prevents the lock by being acquired, it is considered "nice"
// to let a few short-running processes slip in and proceed, then optimistically try
// and acquire the lock again.
func NewTableLock(ctx context.Context, db *sql.DB, table *table.TableInfo, config *DBConfig, logger loggers.Advanced) (*TableLock, error) {
	var err error
	var isFatal bool
	var lockTxn *sql.Tx
	for i := range config.MaxRetries {
		func() {
			lockTxn, _ = db.BeginTx(ctx, nil)
			defer func() {
				if err != nil {
					_ = lockTxn.Rollback()
					if i < config.MaxRetries-1 && !isFatal {
						backoff(i)
					}
				}
			}()
			// We need to lock all the tables we intend to write to while we have the lock.
			// So this is at least the table and the new table, but in the multi-subscription
			// case it could be a lot more tables. If they are not locked, mysql will return
			// an error when trying to write to them.
			// TODO: accept a slice of tables to be locked as a parameter.
			logger.Warnf("trying to acquire table lock, timeout: %d", config.LockWaitTimeout)
			_, err = lockTxn.ExecContext(ctx, fmt.Sprintf("LOCK TABLES %s WRITE, `%s`.`_%s_new` WRITE",
				table.QuotedName,
				table.SchemaName, table.TableName,
			))
			if err != nil {
				// See if the error is retryable, many are
				if !canRetryError(err) {
					isFatal = true
					return
				}
				logger.Warnf("failed trying to acquire table lock, backing off and retrying: %v", err)
				return
			}
		}()
		// check if successful
		if err == nil {
			logger.Warn("table lock acquired")
			return &TableLock{
				table:   table,
				lockTxn: lockTxn,
				logger:  logger,
			}, nil
		}
	}
	// retries exhausted, return the last error
	return nil, err
}

// ExecUnderLock executes a set of statements under a table lock.
func (s *TableLock) ExecUnderLock(ctx context.Context, stmts ...string) error {
	for _, stmt := range stmts {
		if stmt == "" {
			continue
		}
		_, err := s.lockTxn.ExecContext(ctx, stmt)
		if err != nil {
			return err
		}
	}
	return nil
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

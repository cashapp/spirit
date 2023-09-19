package dbconn

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/siddontang/loggers"
	"github.com/squareup/spirit/pkg/table"
	"github.com/squareup/spirit/pkg/utils"
)

type ConnPool struct {
	db     *sql.DB
	config *DBConfig
	conns  chan *sql.Conn
	logger loggers.Advanced
}

// NewPoolWithConsistentSnapshot creates a pool of transactions which have already
// had their read-view created in REPEATABLE READ isolation, and the snapshot
// has been opened.
func NewPoolWithConsistentSnapshot(ctx context.Context, db *sql.DB, count int, config *DBConfig, logger loggers.Advanced) (*ConnPool, error) {
	rrConns := make(chan *sql.Conn, count)
	for i := 0; i < count; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return nil, err
		}
		_, err = conn.ExecContext(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
		if err != nil {
			return nil, err
		}
		_, err = conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
		if err != nil {
			return nil, err
		}
		// Set SQL mode, charset, etc.
		if err := standardizeConn(ctx, conn, config); err != nil {
			return nil, err
		}
		rrConns <- conn
	}
	return &ConnPool{conns: rrConns, config: config, db: db, logger: logger}, nil
}

// NewConnPool creates a pool of connections which have already
// been standardised.
func NewConnPool(ctx context.Context, db *sql.DB, count int, config *DBConfig, logger loggers.Advanced) (*ConnPool, error) {
	conns := make(chan *sql.Conn, count)
	for i := 0; i < count; i++ {
		conn, err := newStandardConnection(ctx, db, config)
		if err != nil {
			return nil, err
		}
		conns <- conn
	}
	return &ConnPool{config: config, conns: conns, db: db, logger: logger}, nil
}

func newStandardConnection(ctx context.Context, db *sql.DB, config *DBConfig) (*sql.Conn, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	// Set SQL mode, charset, etc.
	if err := standardizeConn(ctx, conn, config); err != nil {
		return nil, err
	}
	return conn, nil
}

// DB returns the underlying *sql.DB for the pool, in an un-pooled way.
// This usage is deprecated, and only expected to be used in tests
// and very specific uses.
func (p *ConnPool) DB() *sql.DB {
	return p.db
}

// DBConfig returns the underlying configuration for the pool.
func (p *ConnPool) DBConfig() *DBConfig {
	return p.config
}

// Size of the pool
func (p *ConnPool) Size() int {
	return len(p.conns)
}

// RetryableTransaction uses the first available connection
// it retries all statements in a transaction, retrying if a statement
// errors, or there is a deadlock. It will retry up to maxRetries times.
func (p *ConnPool) RetryableTransaction(ctx context.Context, ignoreDupKeyWarnings bool, stmts ...string) (int64, error) {
	var err error
	var trx *sql.Tx
	var rowsAffected int64
	conn, err := p.Get(ctx)
	if err != nil {
		return 0, err // could not Get connection
	}
	defer p.Put(conn)
RETRYLOOP:
	for i := 0; i < p.config.MaxRetries; i++ {
		select {
		case <-ctx.Done():
			conn = nil // To avoid putting back connection to possibly closed channel
			return rowsAffected, ctx.Err()
		default:
			// Start a transaction
			if trx, err = conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err != nil {
				backoff(i)
				continue RETRYLOOP // retry
			}
			// Execute all statements.
			for _, stmt := range stmts {
				if stmt == "" {
					continue
				}
				var res sql.Result
				if res, err = trx.ExecContext(ctx, stmt); err != nil {
					if canRetryError(err) {
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
	}
	// We failed too many times, return the last error
	return rowsAffected, err
}

// Get gets a connection from the pool.
func (p *ConnPool) Get(ctx context.Context) (*sql.Conn, error) {
	return <-p.conns, nil
}

// GetWithConnectionID gets a connection from the pool along with its ID.
func (p *ConnPool) GetWithConnectionID(ctx context.Context) (*sql.Conn, int, error) {
	conn, err := p.Get(ctx)
	if err != nil {
		return nil, -1, err
	}
	var connectionID int
	err = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&connectionID)
	return conn, connectionID, err
}

// Exec is like db.Exec but it uses the first available connection
// in the connection pool. Connections have previously been standardized.
func (p *ConnPool) Exec(ctx context.Context, query string) error {
	conn, err := p.Get(ctx)
	if err != nil {
		return err
	}
	defer p.Put(conn)
	_, err = conn.ExecContext(ctx, query)
	return err
}

// Put puts a connection back in the pool.
func (p *ConnPool) Put(conn *sql.Conn) {
	if conn == nil {
		return
	}
	p.conns <- conn
}

// Close closes all connection in the pool.
func (p *ConnPool) Close() error {
	close(p.conns)
	for conn := range p.conns {
		// TODO Find a way to do this cleanly.
		// Can't close an already closed connection,
		// so we ignore the error in closing.
		utils.ErrInErr(conn.Close())
	}
	return nil
}

// NewTableLock creates a new server wide lock on a table.
// i.e. LOCK TABLES .. READ.
// It uses a short-timeout with backoff and retry, since if there is a long-running
// process that currently prevents the lock by being acquired, it is considered "nice"
// to let a few short-running processes slip in and proceed, then optimistically try
// and acquire the lock again.
func (p *ConnPool) NewTableLock(ctx context.Context, table *table.TableInfo, writeLock bool) (*TableLock, error) {
	lockTxn, _ := p.db.BeginTx(ctx, nil)
	_, err := lockTxn.ExecContext(ctx, "SET SESSION lock_wait_timeout = ?", p.config.LockWaitTimeout)
	if err != nil {
		return nil, err // could not change timeout.
	}
	lockStmt := "LOCK TABLES " + table.QuotedName + " READ"
	if writeLock {
		lockStmt = fmt.Sprintf("LOCK TABLES %s WRITE, `%s`.`_%s_new` WRITE",
			table.QuotedName,
			table.SchemaName, table.TableName,
		)
	}
	for i := 0; i < p.config.MaxRetries; i++ {
		// In gh-ost they lock the _old table name as well.
		// this might prevent a weird case that we don't handle yet.
		// instead, we DROP IF EXISTS just before the rename, which
		// has a brief race.
		p.logger.Warnf("trying to acquire table lock, timeout: %d", p.config.LockWaitTimeout)
		_, err = lockTxn.ExecContext(ctx, lockStmt)
		if err != nil {
			// See if the error is retryable, many are
			if canRetryError(err) {
				p.logger.Warnf("failed trying to acquire table lock, backing off and retrying: %v", err)
				backoff(i)
				continue
			}
			// else not retryable
			return nil, err
		}
		// else success!
		p.logger.Warn("table lock acquired")
		return &TableLock{
			table:   table,
			lockTxn: lockTxn,
			logger:  p.logger,
		}, nil
	}
	// The loop ended without success.
	// Return the last error
	utils.ErrInErr(lockTxn.Rollback())
	return nil, err
}

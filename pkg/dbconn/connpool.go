package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/squareup/spirit/pkg/utils"
)

type ConnPool struct {
	sync.Mutex
	config *DBConfig
	conns  []*sql.Conn
}

// NewRRConnPool creates a pool of transactions which have already
// had their read-view created in REPEATABLE READ isolation.
func NewRRConnPool(ctx context.Context, db *sql.DB, count int, config *DBConfig) (*ConnPool, error) {
	checksumTxns := make([]*sql.Conn, 0, count)
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
		checksumTxns = append(checksumTxns, conn)
	}
	return &ConnPool{conns: checksumTxns, config: config}, nil
}

// NewConnPool creates a pool of connections which have already
// been standardised.
func NewConnPool(ctx context.Context, db *sql.DB, count int, config *DBConfig) (*ConnPool, error) {
	conns := make([]*sql.Conn, 0, count)
	for i := 0; i < count; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return nil, err
		}
		// Set SQL mode, charset, etc.
		if err := standardizeConn(ctx, conn, config); err != nil {
			return nil, err
		}
		conns = append(conns, conn)
	}
	return &ConnPool{config: config, conns: conns}, nil
}

// RetryableTransaction retries all statements in a transaction, retrying if a statement
// errors, or there is a deadlock. It will retry up to maxRetries times.
func (p *ConnPool) RetryableTransaction(ctx context.Context, ignoreDupKeyWarnings bool, stmts ...string) (int64, error) {
	var err error
	var trx *sql.Tx
	var rowsAffected int64
	conn, err := p.Get()
	if err != nil {
		return 0, err // could not Get connection
	}
	defer p.Put(conn)
RETRYLOOP:
	for i := 0; i < p.config.MaxRetries; i++ {
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
	// We failed too many times, return the last error
	return rowsAffected, err
}

// Get gets a transaction from the pool.
func (p *ConnPool) Get() (*sql.Conn, error) {
	p.Lock()
	defer p.Unlock()
	if len(p.conns) == 0 {
		return nil, errors.New("no conns in pool")
	}
	conn := p.conns[0]
	p.conns = p.conns[1:]
	return conn, nil
}

// Put puts a transaction back in the pool.
func (p *ConnPool) Put(trx *sql.Conn) {
	if trx == nil {
		return
	}
	p.Lock()
	defer p.Unlock()
	p.conns = append(p.conns, trx)
}

// Close closes all transactions in the pool.
func (p *ConnPool) Close() error {
	fmt.Println("ABout to close all connections ####")
	for _, conn := range p.conns {
		if err := conn.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (p *ConnPool) Size() int {
	return len(p.conns)
}

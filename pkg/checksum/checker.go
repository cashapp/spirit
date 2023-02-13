// Package checksum provides online checksum functionality.
// Two tables on the same MySQL server can be compared with only an initial lock.
package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/siddontang/loggers"
	"github.com/squareup/spirit/pkg/dbconn"
	"github.com/squareup/spirit/pkg/repl"
	"github.com/squareup/spirit/pkg/table"
	"github.com/squareup/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"
)

type Checker struct {
	sync.Mutex
	table       *table.TableInfo
	shadowTable *table.TableInfo
	concurrency int
	feed        *repl.Client
	db          *sql.DB
	trxPool     *dbconn.TrxPool
	isInvalid   bool
	logger      loggers.Advanced
}

// NewChecker creates a new checksum object.
func NewChecker(db *sql.DB, table, shadowTable *table.TableInfo, concurrency int, feed *repl.Client, logger loggers.Advanced) (*Checker, error) {
	if concurrency == 0 {
		concurrency = 4
	}
	if feed == nil {
		return nil, errors.New("feed must be non-nil")
	}
	if shadowTable == nil || table == nil {
		return nil, errors.New("table and shadowTable must be non-nil")
	}
	if table.Chunker == nil {
		return nil, errors.New("table must have chunker attached")
	}
	checksum := &Checker{
		table:       table,
		shadowTable: shadowTable,
		concurrency: concurrency,
		db:          db,
		feed:        feed,
		logger:      logger,
	}
	return checksum, nil
}

func (c *Checker) checksumChunk(trxPool *dbconn.TrxPool, chunk *table.Chunk) error {
	trx := trxPool.Get()
	defer trxPool.Put(trx)
	c.logger.Debugf("checksumming chunk: %s", chunk.String())
	source := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum FROM %s WHERE %s",
		utils.IntersectColumns(c.table, c.shadowTable, true),
		c.table.QuotedName(),
		chunk.String(),
	)
	target := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum FROM %s WHERE %s",
		utils.IntersectColumns(c.table, c.shadowTable, true),
		c.shadowTable.QuotedName(),
		chunk.String(),
	)
	var sourceChecksum, targetChecksum int64
	err := trx.QueryRow(source).Scan(&sourceChecksum)
	if err != nil {
		return err
	}
	err = trx.QueryRow(target).Scan(&targetChecksum)
	if err != nil {
		return err
	}
	if sourceChecksum != targetChecksum {
		return fmt.Errorf("checksum mismatch: %v != %v, sourceSQL: %s", sourceChecksum, targetChecksum, source)
	}
	return nil
}

func (c *Checker) isHealthy() bool {
	c.Lock()
	defer c.Unlock()
	return !c.isInvalid
}

func (c *Checker) Run(ctx context.Context) error {
	// Try and catch up before we apply a table lock,
	// since we will need to catch up again with the lock held
	// and we want to minimize that.
	if err := c.feed.BlockWait(); err != nil {
		return err
	}
	// Lock the source table in a trx
	// so the connection is not used by others
	c.logger.Info("starting checksum operation, this will require a table lock")
	serverLock, err := dbconn.NewTableLock(ctx, c.db, c.table, c.logger)
	if err != nil {
		return err
	}
	defer serverLock.Close()

	// To guarantee that Flush() is effective we need to make sure
	// that the canal routine to read the binlog is not delayed. Otherwise
	// we could have a scenario where additional changes arrive after flushChangeSet below
	// That are required for consistency.
	if err := c.feed.BlockWait(); err != nil {
		return err
	}
	// With the lock held, flush one more time under the lock tables.
	// Because we know canal is up to date this now guarantees
	// we have everything in the shadow table.
	if err := c.feed.Flush(ctx); err != nil {
		return err
	}
	// Create a set of connections which can be used to checksum
	// The table. They MUST be created before the lock is released
	// with REPEATABLE-READ and a consistent snapshot (or dummy read)
	// to initialize the read-view.
	c.trxPool, err = dbconn.NewTrxPool(ctx, c.db, c.concurrency)
	if err != nil {
		return err
	}
	// We can now unlock the table before starting the checksumming.
	if err = serverLock.Close(); err != nil {
		return err
	}
	c.logger.Info("table unlocked, starting checksum")

	g := new(errgroup.Group)
	g.SetLimit(c.concurrency)
	for !c.table.Chunker.IsRead() && c.isHealthy() {
		g.Go(func() error {
			chunk, err := c.table.Chunker.Next()
			if err != nil {
				if err == table.ErrTableIsRead {
					return nil
				}
				c.isInvalid = true
				return err
			}
			if err := c.checksumChunk(c.trxPool, chunk); err != nil {
				c.isInvalid = true
				return err
			}
			return nil
		})
	}
	// wait for all work to finish
	err1 := g.Wait()
	// Regardless of err state, we should attempt to rollback the transaction
	// in checksumTxns. They are likely holding metadata locks, which will block
	// further operations like cleanup or cut-over.
	if err := c.trxPool.Close(); err != nil {
		return err
	}
	if err1 != nil {
		c.logger.Error("checksum failed")
		return err1
	}
	return nil
}

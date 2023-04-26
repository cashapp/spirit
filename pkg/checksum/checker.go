// Package checksum provides online checksum functionality.
// Two tables on the same MySQL server can be compared with only an initial lock.
// It is not in the row/ package because it requires a replClient to be passed in,
// which would cause a circular dependency.
package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/siddontang/loggers"
	"github.com/sirupsen/logrus"
	"github.com/squareup/spirit/pkg/dbconn"
	"github.com/squareup/spirit/pkg/repl"
	"github.com/squareup/spirit/pkg/table"
	"github.com/squareup/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"
)

type Checker struct {
	sync.Mutex
	table       *table.TableInfo
	newTable    *table.TableInfo
	concurrency int
	feed        *repl.Client
	db          *sql.DB
	trxPool     *dbconn.TrxPool
	isInvalid   bool
	chunker     table.Chunker
	StartTime   time.Time
	ExecTime    time.Duration
	recentValue interface{} // used for status
	logger      loggers.Advanced
}

type CheckerConfig struct {
	Concurrency     int
	TargetChunkTime time.Duration
	Logger          loggers.Advanced
}

func NewCheckerDefaultConfig() *CheckerConfig {
	return &CheckerConfig{
		Concurrency:     4,
		TargetChunkTime: 1000 * time.Millisecond,
		Logger:          logrus.New(),
	}
}

// NewChecker creates a new checksum object.
func NewChecker(db *sql.DB, tbl, newTable *table.TableInfo, feed *repl.Client, config *CheckerConfig) (*Checker, error) {
	if feed == nil {
		return nil, errors.New("feed must be non-nil")
	}
	if newTable == nil || tbl == nil {
		return nil, errors.New("table and newTable must be non-nil")
	}
	chunker, err := table.NewChunker(tbl, config.TargetChunkTime, config.Logger)
	if err != nil {
		return nil, err
	}
	checksum := &Checker{
		table:       tbl,
		newTable:    newTable,
		concurrency: config.Concurrency,
		db:          db,
		feed:        feed,
		chunker:     chunker,
		logger:      config.Logger,
	}
	return checksum, nil
}

func (c *Checker) checksumChunk(trxPool *dbconn.TrxPool, chunk *table.Chunk) error {
	startTime := time.Now()
	trx, err := trxPool.Get()
	if err != nil {
		return err
	}
	defer trxPool.Put(trx)
	c.logger.Debugf("checksumming chunk: %s", chunk.String())
	source := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum FROM %s WHERE %s",
		utils.IntersectColumns(c.table, c.newTable, true),
		c.table.QuotedName(),
		chunk.String(),
	)
	target := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum FROM %s WHERE %s",
		utils.IntersectColumns(c.table, c.newTable, true),
		c.newTable.QuotedName(),
		chunk.String(),
	)
	var sourceChecksum, targetChecksum int64
	err = trx.QueryRow(source).Scan(&sourceChecksum)
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
	if chunk.LowerBound != nil {
		c.Lock()
		c.recentValue = chunk.LowerBound.Value
		c.Unlock()
	}
	c.chunker.Feedback(chunk, time.Since(startTime))
	return nil
}
func (c *Checker) RecentValue() string {
	return fmt.Sprintf("%v", c.recentValue)
}

func (c *Checker) isHealthy() bool {
	c.Lock()
	defer c.Unlock()
	return !c.isInvalid
}

func (c *Checker) Run(ctx context.Context) error {
	c.StartTime = time.Now()
	defer func() {
		c.ExecTime = time.Since(c.StartTime)
	}()
	if err := c.chunker.Open(); err != nil {
		return err
	}
	// Try and catch up before we apply a table lock,
	// since we will need to catch up again with the lock held
	// and we want to minimize that.
	if err := c.feed.BlockWait(ctx); err != nil {
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
	if err := c.feed.BlockWait(ctx); err != nil {
		return err
	}
	// With the lock held, flush one more time under the lock tables.
	// Because we know canal is up to date this now guarantees
	// we have everything in the new table.
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
	for !c.chunker.IsRead() && c.isHealthy() {
		g.Go(func() error {
			chunk, err := c.chunker.Next()
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

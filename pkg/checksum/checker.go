// Package checksum provides online checksum functionality.
// Two tables on the same MySQL server can be compared with only an initial lock.
// It is not in the row/ package because it requires a replClient to be passed in,
// which would cause a circular dependency.
package checksum

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/siddontang/loggers"
	"github.com/sirupsen/logrus"
	"github.com/squareup/spirit/pkg/dbconn"
	"github.com/squareup/spirit/pkg/repl"
	"github.com/squareup/spirit/pkg/table"
	"golang.org/x/sync/errgroup"
)

type Checker struct {
	sync.Mutex
	table       *table.TableInfo
	newTable    *table.TableInfo
	concurrency int
	feed        *repl.Client
	pool        *dbconn.ConnPool // non snapshot connections
	ssPool      *dbconn.ConnPool // RR snapshot connections
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
	Pool            *dbconn.ConnPool // non snapshot connections
	SSPool          *dbconn.ConnPool // RR snapshot connections, usually nil
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
func NewChecker(tbl, newTable *table.TableInfo, feed *repl.Client, config *CheckerConfig) (*Checker, error) {
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
		ssPool:      config.SSPool, // usually nil
		pool:        config.Pool,
		feed:        feed,
		chunker:     chunker,
		logger:      config.Logger,
	}
	return checksum, nil
}

func (c *Checker) ChecksumChunk(ctx context.Context, chunk *table.Chunk) error {
	startTime := time.Now()
	conn, err := c.ssPool.Get(ctx)
	if err != nil {
		return err
	}
	defer c.ssPool.Put(conn)
	c.logger.Debugf("checksumming chunk: %s", chunk.String())
	source := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum FROM %s WHERE %s",
		c.intersectColumns(),
		c.table.QuotedName,
		chunk.String(),
	)
	target := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum FROM %s WHERE %s",
		c.intersectColumns(),
		c.newTable.QuotedName,
		chunk.String(),
	)
	var sourceChecksum, targetChecksum int64
	err = conn.QueryRowContext(ctx, source).Scan(&sourceChecksum)
	if err != nil {
		return err
	}
	err = conn.QueryRowContext(ctx, target).Scan(&targetChecksum)
	if err != nil {
		return err
	}
	if sourceChecksum != targetChecksum {
		return fmt.Errorf("checksum mismatch: %v != %v, sourceSQL: %s", sourceChecksum, targetChecksum, source)
	}
	if chunk.LowerBound != nil {
		c.Lock()
		// For recent value we only use the first part of the key.
		c.recentValue = chunk.LowerBound.Value[0]
		c.Unlock()
	}
	c.chunker.Feedback(chunk, time.Since(startTime))
	return nil
}
func (c *Checker) RecentValue() string {
	if c.recentValue == nil {
		return "TBD"
	}
	return fmt.Sprintf("%v", c.recentValue)
}

func (c *Checker) isHealthy(ctx context.Context) bool {
	c.Lock()
	defer c.Unlock()
	if ctx.Err() != nil {
		return false
	}
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
	serverLock, err := c.pool.NewTableLock(ctx, c.table, false)
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

	// Assert that the change set is empty. This should always
	// be the case because we are under a lock.
	if c.feed.GetDeltaLen() > 0 {
		return errors.New("the changeset is not empty, can not start checksum")
	}

	// Create a set of connections which can be used to checksum
	// The table. They MUST be created before the lock is released
	// with REPEATABLE-READ and a consistent snapshot (or dummy read)
	// to initialize the read-view.
	c.ssPool, err = dbconn.NewPoolWithConsistentSnapshot(ctx, c.pool.DB(), c.concurrency, c.pool.DBConfig(), c.logger)
	if err != nil {
		return err
	}

	// Assert that the change set is still empty.
	// It should always be empty while we are still under the lock.
	if c.feed.GetDeltaLen() > 0 {
		return errors.New("the changeset is not empty, can not run checksum")
	}

	// We can now unlock the table before starting the checksumming.
	if err = serverLock.Close(); err != nil {
		return err
	}
	c.logger.Info("table unlocked, starting checksum")

	// Start the periodic flush again *just* for the duration of the checksum.
	// If the checksum is long running, it could block flushing for too long:
	// - If we need to resume from checkpoint, the binlogs may not be there.
	// - If they are there, they will take a huge amount of time to flush
	// - The memory requirements for 1MM deltas seems reasonable, but for a multi-day
	//   checksum it is reasonable to assume it may exceed this.
	go c.feed.StartPeriodicFlush(ctx, repl.DefaultFlushInterval)
	defer c.feed.StopPeriodicFlush()

	g, errGrpCtx := errgroup.WithContext(ctx)
	g.SetLimit(c.concurrency)
	for !c.chunker.IsRead() && c.isHealthy(errGrpCtx) {
		g.Go(func() error {
			chunk, err := c.chunker.Next()
			if err != nil {
				if err == table.ErrTableIsRead {
					return nil
				}
				c.isInvalid = true
				return err
			}
			if err := c.ChecksumChunk(ctx, chunk); err != nil {
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
	if err := c.ssPool.Close(); err != nil {
		return err
	}
	if err1 != nil {
		c.logger.Error("checksum failed")
		return err1
	}
	return nil
}

// intersectColumns is similar to utils.IntersectColumns, but it
// wraps an IFNULL(), ISNULL() and cast operation around the columns.
// The cast is to c.newTable type.
func (c *Checker) intersectColumns() string {
	var intersection []string
	for _, col := range c.table.Columns {
		for _, col2 := range c.newTable.Columns {
			if col == col2 {
				// Column exists in both, so we add intersection wrapped in
				// IFNULL, ISNULL and CAST.
				intersection = append(intersection, "IFNULL("+c.newTable.WrapCastType(col)+",''), ISNULL(`"+col+"`)")
			}
		}
	}
	return strings.Join(intersection, ", ")
}

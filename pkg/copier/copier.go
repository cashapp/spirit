// Package copier copies rows from one table to another.
// it makes use of tableinfo.Chunker, and does the parallelism
// and retries here. It fails on the first error.
package copier

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/squareup/spirit/pkg/dbconn"
	"github.com/squareup/spirit/pkg/table"
	"github.com/squareup/spirit/pkg/throttler"
	"github.com/squareup/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"
)

type Copier struct {
	sync.Mutex
	db                *sql.DB
	table             *table.TableInfo
	shadowTable       *table.TableInfo
	concurrency       int
	finalChecksum     bool
	CopyRowsStartTime time.Time
	CopyRowsExecTime  time.Duration
	CopyRowsCount     int64
	CopyChunksCount   int64
	EtaRowsPerSecond  int64
	isInvalid         bool
	Throttler         throttler.Throttler
	logger            log.FieldLogger
}

func NewCopier(db *sql.DB, table, shadowTable *table.TableInfo, concurrency int, finalChecksum bool, logger log.FieldLogger) (*Copier, error) {
	if concurrency == 0 {
		concurrency = 4
	}
	if shadowTable == nil || table == nil {
		return nil, fmt.Errorf("table and shadowTable must be non-nil")
	}
	if table.Chunker == nil {
		return nil, fmt.Errorf("table must have a chunker attached")
	}
	return &Copier{
		db:            db,
		table:         table,
		shadowTable:   shadowTable,
		concurrency:   concurrency,
		finalChecksum: finalChecksum,
		Throttler:     &throttler.Noop{},
		logger:        logger,
	}, nil
}

// MigrateChunk copies a chunk from the table to the shadowTable.
// it is public so it can be used in tests incrementally.
func (c *Copier) MigrateChunk(ctx context.Context, chunk *table.Chunk) error {
	c.Throttler.BlockWait()
	startTime := time.Now()
	// INSERT INGORE because we can have duplicate rows in the chunk because in
	// resuming from checkpoint we will be re-applying some of the previous executed work.
	query := fmt.Sprintf("INSERT IGNORE INTO %s (%s) SELECT %s FROM %s FORCE INDEX (PRIMARY) WHERE %s",
		c.shadowTable.QuotedName(),
		utils.IntersectColumns(c.table, c.shadowTable, false),
		utils.IntersectColumns(c.table, c.shadowTable, false),
		c.table.QuotedName(),
		chunk.String(),
	)
	c.logger.WithFields(log.Fields{
		"chunk": chunk.String(),
		"query": query,
	}).Debug("running chunk")

	var affectedRows int64
	var err error
	if affectedRows, err = dbconn.RetryableTransaction(ctx, c.db, c.finalChecksum, query); err != nil {
		return err
	}
	atomic.AddInt64(&c.CopyRowsCount, affectedRows)
	atomic.AddInt64(&c.CopyChunksCount, 1)
	// Send feedback which can be used by the chunker
	// and infoschema to create a low watermark.
	c.table.Chunker.Feedback(chunk, time.Since(startTime))
	return nil
}

func (c *Copier) isHealthy() bool {
	c.Lock()
	defer c.Unlock()
	return !c.isInvalid
}

func (c *Copier) Run(ctx context.Context) error {
	c.CopyRowsStartTime = time.Now()
	defer func() {
		c.logger.Info("copy rows complete")
		c.CopyRowsExecTime = time.Since(c.CopyRowsStartTime)
	}()
	g, ctx := errgroup.WithContext(ctx)
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
			if err := c.MigrateChunk(ctx, chunk); err != nil {
				c.isInvalid = true
				return err
			}
			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (c *Copier) SetThrottler(throttler throttler.Throttler) {
	c.Lock()
	defer c.Unlock()
	c.Throttler = throttler
}

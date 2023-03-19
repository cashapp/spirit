// Package row copies rows from one table to another.
// it makes use of tableinfo.Chunker, and does the parallelism
// and retries here. It fails on the first error.
package row

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/siddontang/go-log/loggers"
	"github.com/sirupsen/logrus"
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
	chunker           table.Chunker
	concurrency       int
	finalChecksum     bool
	CopyRowsStartTime time.Time
	CopyRowsExecTime  time.Duration
	CopyRowsCount     int64
	CopyChunksCount   int64
	EtaRowsPerSecond  int64
	isInvalid         bool
	Throttler         throttler.Throttler
	logger            loggers.Advanced
}

type CopierConfig struct {
	Concurrency           int
	TargetMs              int64
	FinalChecksum         bool
	DisableTrivialChunker bool
	Throttler             throttler.Throttler
	Logger                loggers.Advanced
}

// NewCopierDefaultConfig returns a default config for the copier.
func NewCopierDefaultConfig() *CopierConfig {
	return &CopierConfig{
		Concurrency:           4,
		TargetMs:              1000,
		FinalChecksum:         true,
		DisableTrivialChunker: false,
		Throttler:             &throttler.Noop{},
		Logger:                logrus.New(),
	}
}

// NewCopier creates a new copier object.
func NewCopier(db *sql.DB, tbl, shadowTable *table.TableInfo, config *CopierConfig) (*Copier, error) {
	if shadowTable == nil || tbl == nil {
		return nil, errors.New("table and shadowTable must be non-nil")
	}
	chunker, err := table.NewChunker(tbl, config.TargetMs, config.DisableTrivialChunker, config.Logger)
	if err != nil {
		return nil, err
	}
	return &Copier{
		db:            db,
		table:         tbl,
		shadowTable:   shadowTable,
		concurrency:   config.Concurrency,
		finalChecksum: config.FinalChecksum,
		Throttler:     config.Throttler,
		chunker:       chunker,
		logger:        config.Logger,
	}, nil
}

// NewCopierFromCheckpoint creates a new copier object, from a checkpoint (copyRowsAt, copyRows)
func NewCopierFromCheckpoint(db *sql.DB, tbl, shadowTable *table.TableInfo, config *CopierConfig, copyRowsAt string, copyRows int64) (*Copier, error) {
	c, err := NewCopier(db, tbl, shadowTable, config)
	if err != nil {
		return c, err
	}
	// Overwrite the previously attached chunker with one at a specific watermark.
	if err := c.chunker.OpenAtWatermark(copyRowsAt); err != nil {
		return c, err
	}
	// Success from this point on
	// Overwrite copy-rows
	atomic.StoreInt64(&c.CopyRowsCount, copyRows)
	return c, nil
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
	c.logger.Debugf("running chunk: %s, query: %s", chunk.String(), query)
	var affectedRows int64
	var err error
	if affectedRows, err = dbconn.RetryableTransaction(ctx, c.db, c.finalChecksum, query); err != nil {
		return err
	}
	atomic.AddInt64(&c.CopyRowsCount, affectedRows)
	atomic.AddInt64(&c.CopyChunksCount, 1)
	// Send feedback which can be used by the chunker
	// and infoschema to create a low watermark.
	c.chunker.Feedback(chunk, time.Since(startTime))
	return nil
}

func (c *Copier) isHealthy() bool {
	c.Lock()
	defer c.Unlock()
	return !c.isInvalid
}

func (c *Copier) Run(ctx context.Context) error {
	c.CopyRowsStartTime = time.Now()
	if err := c.chunker.Open(); err != nil {
		return err
	}
	defer func() {
		c.CopyRowsExecTime = time.Since(c.CopyRowsStartTime)
	}()
	g, ctx := errgroup.WithContext(ctx)
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

// The following funcs proxy to the chunker.
// This is done so we don't need to export the chunker,

// KeyAboveHighWatermark returns true if the key is above where the chunker is currently at.
func (c *Copier) KeyAboveHighWatermark(key interface{}) bool {
	return c.chunker.KeyAboveHighWatermark(key)
}

// GetLowWatermark returns the low watermark of the chunker, i.e. the lowest key that has been
// guaranteed to be written to the shadow table.
func (c *Copier) GetLowWatermark() (string, error) {
	return c.chunker.GetLowWatermark()
}

// Next4Test is typically only used in integration tests that don't want to actually migrate data,
// but need to advance the chunker.
func (c *Copier) Next4Test() (*table.Chunk, error) {
	return c.chunker.Next()
}

// Open4Test is typically only used in integration tests that don't want to actually migrate data,
// but need to open the chunker.
func (c *Copier) Open4Test() error {
	return c.chunker.Open()
}

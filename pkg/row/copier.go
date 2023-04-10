// Package row copies rows from one table to another.
// it makes use of tableinfo.Chunker, and does the parallelism
// and retries here. It fails on the first error.
package row

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
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

const (
	copyEstimateInterval   = 10 * time.Second // how frequently to re-estimate copy speed
	copyETAInitialWaitTime = 1 * time.Minute  // how long to wait before first estimating copy speed (to allow for fast start)
)

type Copier struct {
	sync.Mutex
	db                   *sql.DB
	table                *table.TableInfo
	newTable             *table.TableInfo
	chunker              table.Chunker
	concurrency          int
	finalChecksum        bool
	CopyRowsStartTime    time.Time
	CopyRowsExecTime     time.Duration
	CopyRowsCount        uint64 // used for estimates: the exact number of rows copied
	CopyRowsLogicalCount uint64 // used for estimates on auto-inc PKs: rows copied including any gaps
	CopyChunksCount      uint64
	rowsPerSecond        uint64
	isInvalid            bool
	isOpen               bool
	StartTime            time.Time
	ExecTime             time.Duration
	Throttler            throttler.Throttler
	logger               loggers.Advanced
}

type CopierConfig struct {
	Concurrency           int
	TargetChunkTime       time.Duration
	FinalChecksum         bool
	DisableTrivialChunker bool
	Throttler             throttler.Throttler
	Logger                loggers.Advanced
}

// NewCopierDefaultConfig returns a default config for the copier.
func NewCopierDefaultConfig() *CopierConfig {
	return &CopierConfig{
		Concurrency:           4,
		TargetChunkTime:       1000 * time.Millisecond,
		FinalChecksum:         true,
		DisableTrivialChunker: false,
		Throttler:             &throttler.Noop{},
		Logger:                logrus.New(),
	}
}

// NewCopier creates a new copier object.
func NewCopier(db *sql.DB, tbl, newTable *table.TableInfo, config *CopierConfig) (*Copier, error) {
	if newTable == nil || tbl == nil {
		return nil, errors.New("table and newTable must be non-nil")
	}
	chunker, err := table.NewChunker(tbl, config.TargetChunkTime, config.DisableTrivialChunker, config.Logger)
	if err != nil {
		return nil, err
	}
	return &Copier{
		db:            db,
		table:         tbl,
		newTable:      newTable,
		concurrency:   config.Concurrency,
		finalChecksum: config.FinalChecksum,
		Throttler:     config.Throttler,
		chunker:       chunker,
		logger:        config.Logger,
	}, nil
}

// NewCopierFromCheckpoint creates a new copier object, from a checkpoint (copyRowsAt, copyRows)
func NewCopierFromCheckpoint(db *sql.DB, tbl, newTable *table.TableInfo, config *CopierConfig, lowWatermark string, rowsCopied uint64, rowsCopiedLogical uint64) (*Copier, error) {
	c, err := NewCopier(db, tbl, newTable, config)
	if err != nil {
		return c, err
	}
	// Overwrite the previously attached chunker with one at a specific watermark.
	if err := c.chunker.OpenAtWatermark(lowWatermark); err != nil {
		return c, err
	}
	c.isOpen = true
	// Success from this point on
	// Overwrite copy-rows
	atomic.StoreUint64(&c.CopyRowsCount, rowsCopied)
	atomic.StoreUint64(&c.CopyRowsLogicalCount, rowsCopiedLogical)
	return c, nil
}

// CopyChunk copies a chunk from the table to the newTable.
// it is public so it can be used in tests incrementally.
func (c *Copier) CopyChunk(ctx context.Context, chunk *table.Chunk) error {
	c.Throttler.BlockWait()
	startTime := time.Now()
	// INSERT INGORE because we can have duplicate rows in the chunk because in
	// resuming from checkpoint we will be re-applying some of the previous executed work.
	query := fmt.Sprintf("INSERT IGNORE INTO %s (%s) SELECT %s FROM %s FORCE INDEX (PRIMARY) WHERE %s",
		c.newTable.QuotedName(),
		utils.IntersectColumns(c.table, c.newTable, false),
		utils.IntersectColumns(c.table, c.newTable, false),
		c.table.QuotedName(),
		chunk.String(),
	)
	c.logger.Debugf("running chunk: %s, query: %s", chunk.String(), query)
	var affectedRows int64
	var err error
	if affectedRows, err = dbconn.RetryableTransaction(ctx, c.db, c.finalChecksum, query); err != nil {
		return err
	}
	atomic.AddUint64(&c.CopyRowsCount, uint64(affectedRows))
	atomic.AddUint64(&c.CopyRowsLogicalCount, chunk.ChunkSize)
	atomic.AddUint64(&c.CopyChunksCount, 1)
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
	c.StartTime = time.Now()
	defer func() {
		c.ExecTime = time.Since(c.StartTime)
	}()
	if !c.isOpen {
		// For practical reasons resume-from-checkpoint
		// will already be open, new copy processes will not be.
		if err := c.chunker.Open(); err != nil {
			return err
		}
	}
	go c.estimateRowsPerSecondLoop() // estimate rows while copying
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
			if err := c.CopyChunk(ctx, chunk); err != nil {
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

func (c *Copier) getCopyStats() (uint64, uint64, float64) {
	if c.table.PrimaryKeyIsAutoInc {
		// If the table has an autoinc column we use a different estimation method,
		// which tends to be more accurate. We use the maxValue as the estimated rows,
		// and the "logical copied rows" (which is the sum of the chunk sizes) as the
		// rows copied so far.
		copyRows := atomic.LoadUint64(&c.CopyRowsLogicalCount)
		maxValue, err := strconv.ParseUint(c.table.MaxValue(), 10, 64)
		if err != nil {
			maxValue = c.table.EstimatedRows
		}
		pct := float64(copyRows) / float64(maxValue) * 100
		return copyRows, maxValue, pct
	}
	// This is the legacy estimation method, which is not as accurate as the one above.
	// It is required for scenarios like VARBINARY primary keys. The downside here is that
	// the estimated rows can jump around a lot on a big table with a high variability of row size.
	// Because we include the CopyRowsCount to users at least it will *at least*
	// appear like it is always progressing.
	pct := float64(atomic.LoadUint64(&c.CopyRowsCount)) / float64(c.table.EstimatedRows) * 100
	return atomic.LoadUint64(&c.CopyRowsCount), c.table.EstimatedRows, pct
}

// GetProgress returns the progress of the copier
func (c *Copier) GetProgress() string {
	copied, total, pct := c.getCopyStats()
	return fmt.Sprintf("%d/%d %.2f%%", copied, total, pct)
}

func (c *Copier) GetETA() string {
	copiedRows, totalRows, pct := c.getCopyStats()
	rowsPerSecond := atomic.LoadUint64(&c.rowsPerSecond)
	if pct > 99.99 {
		return "DUE"
	}
	if rowsPerSecond == 0 || time.Since(c.StartTime) < copyETAInitialWaitTime {
		return "TBD"
	}
	// divide the remaining rows by how many rows we copied in the last interval per second
	// "remainingRows" might be the actual rows or the logical rows since
	// c.getCopyStats() and rowsPerSecond change estimation method when the PK is auto-inc.
	remainingRows := totalRows - copiedRows
	remainingSeconds := math.Floor(float64(remainingRows) / float64(rowsPerSecond))
	return time.Duration(remainingSeconds * float64(time.Second)).String()
}

func (c *Copier) estimateRowsPerSecondLoop() {
	// We take >10 second averages because with parallel copy it bounces around a lot.
	// If it's an auto-inc key we use the "logical copy rows", because the estimate
	// will be based on the max value of the auto-inc column.
	prevRowsCount := atomic.LoadUint64(&c.CopyRowsCount)
	if c.table.PrimaryKeyIsAutoInc {
		prevRowsCount = atomic.LoadUint64(&c.CopyRowsLogicalCount)
	}
	ticker := time.NewTicker(copyEstimateInterval)
	defer ticker.Stop()
	for range ticker.C {
		if !c.isHealthy() {
			return
		}
		newRowsCount := atomic.LoadUint64(&c.CopyRowsCount)
		if c.table.PrimaryKeyIsAutoInc {
			newRowsCount = atomic.LoadUint64(&c.CopyRowsLogicalCount)
		}
		rowsPerInterval := float64(newRowsCount - prevRowsCount)
		intervalsDivisor := float64(copyEstimateInterval / time.Second) // should be something like 10 for 10 seconds
		rowsPerSecond := uint64(rowsPerInterval / intervalsDivisor)
		atomic.StoreUint64(&c.rowsPerSecond, rowsPerSecond)
		prevRowsCount = newRowsCount
	}
}

// The following funcs proxy to the chunker.
// This is done so we don't need to export the chunker,

// KeyAboveHighWatermark returns true if the key is above where the chunker is currently at.
func (c *Copier) KeyAboveHighWatermark(key interface{}) bool {
	return c.chunker.KeyAboveHighWatermark(key)
}

// GetLowWatermark returns the low watermark of the chunker, i.e. the lowest key that has been
// guaranteed to be written to the new table.
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

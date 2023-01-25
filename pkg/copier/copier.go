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

	"github.com/squareup/gap-core/log"
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
	logger            *log.Logger
}

func NewCopier(db *sql.DB, table, shadowTable *table.TableInfo, concurrency int, finalChecksum bool, logger *log.Logger) (*Copier, error) {
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

	// Attempt to execute the query in a transaction,
	// so we can set the SQL mode and run SHOW WARNINGS after.
	trx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	// Set the TZ, SQL mode and charset.
	if err := dbconn.StandardizeTrx(trx); err != nil {
		return err
	}
	var res sql.Result
	res, err = dbconn.TrxExecWithRetry(trx, query)
	if err != nil {
		// Error is non-retryable or we've retried too many times.
		c.logger.WithFields(log.Fields{
			"chunk": chunk.String(),
			"query": query,
			"error": err,
		}).Error("error migrating chunk")
		return err
	}
	// Even though there was no ERROR we still need to inspect SHOW WARNINGS
	// Because we've intentionally used INSERT IGNORE. We can skip duplicate
	// key errors, but any other error is considered unsafe.
	warningRes, err := trx.Query("SHOW WARNINGS") //nolint: execinquery
	if err != nil {
		return err
	}
	var level, code, message string
	for warningRes.Next() {
		err = warningRes.Scan(&level, &code, &message)
		if err != nil {
			return err
		}
		switch code {
		case "1062":
			// We only permit duplicate key errors if a final checksum is enabled.
			// Duplicate key errors are typical if we are resuming from a checkpoint,
			// *but* they could also be real errors if the DDL being applied is a UNIQUE
			// CONSTRAINT and we miss that.
			if c.finalChecksum {
				continue
			}
			c.logger.WithFields(log.Fields{
				"chunk":   chunk.String(),
				"query":   query,
				"level":   level,
				"code":    code,
				"message": message,
			}).Error("duplicate key error while migrating chunk, and checksum is disabled")
			return fmt.Errorf("duplicate key error while migrating chunk, and checksum is disabled: %s", message)
		default:
			// Any other error is considered unsafe.
			c.logger.WithFields(log.Fields{
				"chunk":   chunk.String(),
				"query":   query,
				"level":   level,
				"code":    code,
				"message": message,
			}).Error("unsafe warning migrating chunk")
			return fmt.Errorf("unsafe warning migrating chunk: %s", message)
		}
	}
	// Success!
	err = trx.Commit()
	if err != nil {
		return err
	}
	count, err := res.RowsAffected()
	if err == nil {
		atomic.AddInt64(&c.CopyRowsCount, count)
	}
	atomic.AddInt64(&c.CopyChunksCount, 1)
	// Send feedback which can be used by the chunker
	// and infoschema to create a low watermark.
	// it's not an error if we can't get row count
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

package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/siddontang/loggers"

	"github.com/squareup/spirit/pkg/dbconn"
	"github.com/squareup/spirit/pkg/repl"
	"github.com/squareup/spirit/pkg/table"
	"golang.org/x/sync/errgroup"
)

type CutOver struct {
	db          *sql.DB
	table       *table.TableInfo
	shadowTable *table.TableInfo
	feed        *repl.Client
	logger      loggers.Advanced
}

const (
	maxRetries = 5
)

// NewCutOver contains the logic to perform the final cut over. It requires the original table,
// shadow table, and a replication feed which is used to ensure consistency before the cut over.
func NewCutOver(db *sql.DB, table, shadowTable *table.TableInfo, feed *repl.Client, logger loggers.Advanced) (*CutOver, error) {
	if feed == nil {
		return nil, errors.New("feed must be non-nil")
	}
	if table == nil || shadowTable == nil {
		return nil, errors.New("table and shadowTable must be non-nil")
	}
	return &CutOver{
		db:          db,
		table:       table,
		shadowTable: shadowTable,
		feed:        feed,
		logger:      logger,
	}, nil
}

func (c *CutOver) Run(ctx context.Context) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		c.logger.Warnf("Attempting final cut over operation (attempt %d/%d)", i+1, maxRetries)
		if err = c.cutover(ctx); err != nil {
			c.logger.Warnf("cutover failed. err: %s", err.Error())
			continue
		}
		c.logger.Warn("final cut over operation complete")
		return nil
	}
	c.logger.Error("cutover failed, and retries exhausted")
	return err
}

func (c *CutOver) cutover(ctx context.Context) error {
	// Try and catch up before we apply a table lock,
	// since we will need to catch up again with the lock held
	// and we want to minimize that.
	if err := c.feed.BlockWait(ctx); err != nil {
		return err
	}
	// Lock the source table in a trx
	// so the connection is not used by others
	serverLock, err := dbconn.NewTableLock(ctx, c.db, c.table, c.logger)
	if err != nil {
		return err
	}
	defer serverLock.Close()

	// Before we flush the change set we need to make sure
	// canal is equal to at least the binary log position we read.
	if err := c.feed.BlockWait(ctx); err != nil {
		return err
	}
	// With the lock held, flush one more time under the lock tables.
	// This guarantees we have everything in the shadow table.
	if err := c.feed.Flush(ctx); err != nil {
		return err
	}
	// In a new connection, swap the tables.
	// In gh-ost they ensure that _old is pre-created and locked (aka a sentry table).
	// This helps prevent an issue where the rename fails. Instead, just before
	// the cutover.Run() executes, we DROP the _old table if it exists. This leaves
	// a very brief race, which is unlikely.
	// gh-ost also uses a LOCK TABLES .. WRITE which is a stronger lock than used here (LOCK TABLES .. READ)
	// so there is a reasonable chance that on a busy system the TableLock is successful but the rename fails.
	// Rather than try and upgrade the lock, we instead try and retry the cut-over operation
	// again in a loop. This seems like a reasonable trade-off.
	g := new(errgroup.Group)
	g.Go(func() error {
		oldName := fmt.Sprintf("_%s_old", c.table.TableName)
		oldQuotedName := fmt.Sprintf("`%s`.`%s`", c.table.SchemaName, oldName)
		query := fmt.Sprintf("RENAME TABLE %s TO %s, %s TO %s",
			c.table.QuotedName(), oldQuotedName,
			c.shadowTable.QuotedName(), c.table.QuotedName())
		return dbconn.DBExec(ctx, c.db, query)
	})
	// We can now unlock the table to allow the rename to go through.
	// Include a ROLLBACK before returning because of MDL.
	if err = serverLock.Close(); err != nil {
		return err
	}
	// Wait for the rename to complete.
	if err := g.Wait(); err != nil {
		return err // rename not successful.
	}
	return nil
}

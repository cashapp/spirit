package migration

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/squareup/gap-core/log"

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
	logger      *log.Logger
}

// NewCutOver contains the logic to perform the final cut over. It requires the original table,
// shadow table, and a replication feed which is used to ensure consistency before the cut over.
func NewCutOver(db *sql.DB, table, shadowTable *table.TableInfo, feed *repl.Client, logger *log.Logger) (*CutOver, error) {
	if feed == nil {
		return nil, fmt.Errorf("feed must be non-nil")
	}
	if table == nil || shadowTable == nil {
		return nil, fmt.Errorf("table and shadowTable must be non-nil")
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
	// Try and catch up before we apply a table lock,
	// since we will need to catch up again with the lock held
	// and we want to minimize that.
	if err := c.feed.BlockWait(); err != nil {
		return err
	}
	// Lock the source table in a trx
	// so the connection is not used by others
	c.logger.Info("Running final cut over operation")
	serverLock, err := dbconn.NewTableLock(ctx, c.db, c.table, c.logger)
	if err != nil {
		return err
	}
	defer serverLock.Close()

	// Before we flush the change set we need to make sure
	// canal is equal to at least the binary log position we read.
	if err := c.feed.BlockWait(); err != nil {
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
	g := new(errgroup.Group)
	g.Go(func() error {
		oldTableName := fmt.Sprintf("`%s`.`%s`", c.table.SchemaName, c.table.TableName+"_old")
		query := fmt.Sprintf("RENAME TABLE %s TO %s, %s TO %s",
			c.table.QuotedName(), oldTableName,
			c.shadowTable.QuotedName(), c.table.QuotedName())
		_, err := c.db.Exec(query)
		return err
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
	c.logger.Info("Final cut over operation complete")
	return nil
}

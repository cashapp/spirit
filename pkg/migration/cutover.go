package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/siddontang/loggers"

	"github.com/squareup/spirit/pkg/dbconn"
	"github.com/squareup/spirit/pkg/repl"
	"github.com/squareup/spirit/pkg/table"
	"github.com/squareup/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"
)

type CutoverAlgorithm int

const (
	Undefined       CutoverAlgorithm = iota
	RenameUnderLock                  // MySQL 8.0 only (best option)
	Ghost                            // As close to gh-ost as possible
	Facebook                         // Has a table not found race, but simpler than gh-ost.
)

func (a CutoverAlgorithm) String() string {
	switch a {
	case RenameUnderLock:
		return "rename-under-lock"
	case Facebook:
		return "facebook"
	default:
		return "gh-ost atomic cutover"
	}
}

type CutOver struct {
	db        *sql.DB
	table     *table.TableInfo
	newTable  *table.TableInfo
	sentry    *table.TableInfo // same as _old table.
	feed      *repl.Client
	algorithm CutoverAlgorithm // RenameUnderLock, Ghost, Facebook
	dbConfig  *dbconn.DBConfig
	logger    loggers.Advanced
}

// NewCutOver contains the logic to perform the final cut over. It requires the original table,
// new table, and a replication feed which is used to ensure consistency before the cut over.
func NewCutOver(db *sql.DB, table, newTable *table.TableInfo, feed *repl.Client, dbConfig *dbconn.DBConfig, logger loggers.Advanced) (*CutOver, error) {
	if feed == nil {
		return nil, errors.New("feed must be non-nil")
	}
	if table == nil || newTable == nil {
		return nil, errors.New("table and newTable must be non-nil")
	}
	// The algorithm is not user-configurable, but tests might try either.
	// For users we try to default to RenameUnderLock but fall back to Ghost
	// if it's 5.7 or there is an error.
	algorithm := RenameUnderLock // default to rename under lock
	if !utils.IsMySQL8(db) {
		algorithm = Ghost
	}
	return &CutOver{
		db:        db,
		table:     table,
		newTable:  newTable,
		feed:      feed,
		dbConfig:  dbConfig,
		algorithm: algorithm,
		logger:    logger,
	}, nil
}

func (c *CutOver) Run(ctx context.Context) error {
	// Create the "magic" (aka sentry table in the source), which
	// is really the same as the old table.
	err := c.createSentryTable(ctx)
	if err != nil {
		return err
	}
	for i := 0; i < c.dbConfig.MaxRetries; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Try and catch up before we attempt the cutover.
		// since we will need to catch up again with the lock held
		// and we want to minimize that.
		if err := c.feed.Flush(ctx); err != nil {
			return err
		}
		// We use maxCutoverRetries as our retrycount, but nested
		// within c.cutoverMySQLX() it may also have a retry for the specific statement
		c.logger.Warnf("Attempting final cut over operation (attempt %d/%d)", i+1, c.dbConfig.MaxRetries)
		c.logger.Infof("Using cutover algorithm: %s", c.algorithm.String())
		switch c.algorithm {
		case RenameUnderLock:
			err = c.algorithmRenameUnderLock(ctx)
		case Facebook:
			err = c.algorithmFacebook(ctx)
		default:
			err = c.algorithmGhostAtomicRename(ctx)
		}
		if err != nil {
			c.logger.Warnf("cutover failed. err: %s", err.Error())
			continue
		}
		c.logger.Warn("final cut over operation complete")
		return nil
	}
	c.logger.Error("cutover failed, and retries exhausted")
	return err
}

// algorithmRenameUnderLock is the preferred cutover algorithm.
// As of MySQL 8.0.13, you can rename tables locked with a LOCK TABLES statement
// https://dev.mysql.com/worklog/task/?id=9826
func (c *CutOver) algorithmRenameUnderLock(ctx context.Context) error {
	// Lock the source table in a trx
	// so the connection is not used by others
	serverLock, err := dbconn.NewTableLock(ctx, c.db, c.table, true, c.dbConfig, c.logger)
	if err != nil {
		return err
	}
	defer serverLock.Close()
	if err := c.feed.FlushUnderLock(ctx, serverLock); err != nil {
		return err
	}
	if !c.feed.AllChangesFlushed() {
		return errors.New("not all changes flushed, final flush might be broken")
	}
	if c.feed.GetDeltaLen() > 0 {
		return fmt.Errorf("the changeset is not empty (%d), can not start cutover", c.feed.GetDeltaLen())
	}
	dropSentryTable := fmt.Sprintf("DROP TABLE IF EXISTS %s", c.sentry.QuotedName)
	renameStatement := fmt.Sprintf("RENAME TABLE %s TO %s, %s TO %s",
		c.table.QuotedName, c.sentry.QuotedName,
		c.newTable.QuotedName, c.table.QuotedName,
	)
	return serverLock.ExecUnderLock(ctx, []string{dropSentryTable, renameStatement})
}

// algorithmFacebook implements the two-step cutover algorithm as defined
// by Facebook's online schema change tool. It has a non-ideal limitation
// where a table does not exist error might be returned, but it looks
// safer than gh-ost's algorithm, so for 5.7 I intend to switch to it
// in the future.
func (c *CutOver) algorithmFacebook(ctx context.Context) error {
	serverLock, err := dbconn.NewTableLock(ctx, c.db, c.table, true, c.dbConfig, c.logger)
	if err != nil {
		return err
	}
	defer serverLock.Close()
	if err := c.feed.FlushUnderLock(ctx, serverLock); err != nil {
		return err
	}
	if !c.feed.AllChangesFlushed() {
		return errors.New("not all changes flushed, final flush might be broken")
	}
	if c.feed.GetDeltaLen() > 0 {
		return fmt.Errorf("the changeset is not empty (%d), can not start cutover", c.feed.GetDeltaLen())
	}
	// We rename the tables (separate connection), and then release the server lock.
	// There will be a brief period of stray updates we can flush, and then we can
	// rename the _new table to the original table name.
	trx, _, err := dbconn.BeginStandardTrx(ctx, c.db, c.dbConfig)
	if err != nil {
		return err
	}
	g, errGrpCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		query := fmt.Sprintf("RENAME TABLE %s TO %s",
			c.table.QuotedName, c.sentry.QuotedName,
		)
		_, err = trx.ExecContext(errGrpCtx, query)
		return err
	})
	// drop the sentry table.
	dropSentryTable := fmt.Sprintf("DROP TABLE IF EXISTS %s", c.sentry.QuotedName)
	if err := serverLock.ExecUnderLock(ctx, []string{dropSentryTable}); err != nil {
		return err
	}
	// Release the lock
	if err = serverLock.Close(); err != nil {
		return err
	}
	// Ensure the rename succeeded
	if err := g.Wait(); err != nil {
		return err // rename not successful.
	}
	// We now have a brief period where stray updates can occur
	// Users are receiving table not found errors during this time.
	if err := c.feed.Flush(ctx); err != nil {
		return err // could not flush?
	}
	// We now do the final rename. It should be quick because nobody
	// Can query the table right now.
	query := fmt.Sprintf("RENAME TABLE %s TO %s",
		c.newTable.QuotedName, c.table.QuotedName,
	)
	_, err = dbconn.RetryableTransaction(ctx, c.db, false, c.dbConfig, query)
	return err
}

// algorithmGhostAtomicRename is the gh-ost cutover algorithm
// as defined at https://github.com/github/gh-ost/issues/82
// To my knowledge, this algorithm can cause data-loss:
//   - in between the UNLOCK and RENAME TABLE some statements may be permitted to insert.
//   - gh-ost relies on an undocumented behavior of MySQL such that the RENAME will take
//     precedence over the inserts (this makes sense from MySQL's perspective to prevent
//     lock starvation).
//   - With a high throughput of inserts, I have been able to reproduce this. i.e.
//     ./finch ../../benchmarks/xfer/setup.yaml
//     (run a migration)
//   - I have been able to reproduce this in both gh-ost and spirit, so it does
//     not appear to be an implementation bug.
func (c *CutOver) algorithmGhostAtomicRename(ctx context.Context) error {
	// LOCK the source table in a trx and start to flush final changes.
	serverLock, err := dbconn.NewTableLock(ctx, c.db, c.table, true, c.dbConfig, c.logger)
	if err != nil {
		return err
	}
	defer serverLock.Close()
	// Start the RENAME TABLE trx. This connection is
	// described as C20 in the gh-ost docs.
	trx, connectionID, err := dbconn.BeginStandardTrx(ctx, c.db, c.dbConfig)
	if err != nil {
		return err
	}
	// Start the rename operation, it's OK it will block inside
	// of this go-routine.
	g, errGrpCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		query := fmt.Sprintf("RENAME TABLE %s TO %s, %s TO %s",
			c.table.QuotedName, c.sentry.QuotedName,
			c.newTable.QuotedName, c.table.QuotedName)
		_, err = trx.ExecContext(errGrpCtx, query)
		return err
	})
	// Flush all changes exhaustively. Because this is under a lock,
	// no new changes can arrive.
	if err := c.feed.FlushUnderLock(ctx, serverLock); err != nil {
		return err
	}
	// These are safety measures to ensure that there are no pending changes.
	// They are not known to return errors, but we check them anyway in case
	// a change of logic is introduced.
	if !c.feed.AllChangesFlushed() {
		return errors.New("not all changes flushed, final flush might be broken")
	}
	if c.feed.GetDeltaLen() > 0 {
		return fmt.Errorf("the changeset is not empty (%d), can not start cutover", c.feed.GetDeltaLen())
	}
	// Check that the rename connection is alive and blocked in SHOW PROCESSLIST
	// If this is TRUE then c10 can DROP TABLE tbl_old and then UNLOCK TABLES.
	if err := c.checkProcesslistForID(ctx, connectionID); err != nil {
		return err
	}
	// From connection C10 we can now DROP the sentry table.
	// Then again from C10 we can release the server lock.
	dropStmt := fmt.Sprintf("DROP TABLE %s", c.sentry.QuotedName)
	if err = serverLock.ExecUnderLock(ctx, []string{dropStmt}); err != nil {
		return err
	}
	if err = serverLock.Close(); err != nil {
		return err
	}
	// Wait for the rename to complete from C20
	if err := g.Wait(); err != nil {
		return err // rename not successful.
	}
	return nil
}

func (c *CutOver) createSentryTable(ctx context.Context) error {
	sentryName := fmt.Sprintf("_%s_old", c.table.TableName)
	_, err := c.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", sentryName))
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (a int not null primary key)", sentryName))
	if err != nil {
		return err
	}
	c.sentry = table.NewTableInfo(c.db, c.table.SchemaName, sentryName)
	return c.sentry.SetInfo(ctx)
}

func (c *CutOver) checkProcesslistForID(ctx context.Context, id int) error {
	var state string
	// try up to 100 times. This can be racey
	for i := 0; i < 100; i++ {
		err := c.db.QueryRowContext(ctx, "SELECT state FROM information_schema.processlist WHERE id = ? AND state = 'Waiting for table metadata lock'", id).Scan(&state)
		if err != nil {
			c.logger.Warnf("error checking processlist for id %d. Err: %s State: %s", id, err.Error(), state)
			time.Sleep(time.Second)
			continue
		}
		return nil
	}
	return fmt.Errorf("processlist id %d is not in the correct state", id)
}

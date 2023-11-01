package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/siddontang/loggers"

	"github.com/cashapp/spirit/pkg/dbconn"
	"github.com/cashapp/spirit/pkg/repl"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/cashapp/spirit/pkg/utils"
)

type CutoverAlgorithm int

const (
	Undefined       CutoverAlgorithm = iota
	RenameUnderLock                  // MySQL 8.0 only (best option)
	Ghost                            // As close to gh-ost as possible
)

func (a CutoverAlgorithm) String() string {
	switch a {
	case RenameUnderLock:
		return "rename-under-lock"
	default:
		return "gh-ost"
	}
}

type CutOver struct {
	db        *sql.DB
	table     *table.TableInfo
	newTable  *table.TableInfo
	feed      *repl.Client
	algorithm CutoverAlgorithm // RenameUnderLock, Ghost
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
	var err error
	if c.dbConfig.MaxOpenConnections < 5 {
		// The gh-ost cutover algorithm requires a minimum of 3 connections:
		// - The LOCK TABLES connection
		// - The RENAME TABLE connection
		// - The Flush() threads
		// Because we want to safely flush quickly, we set the limit to 5.
		c.db.SetMaxOpenConns(5)
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
		// within c.algorithmX() it may also have a retry for the specific statement
		c.logger.Warnf("Attempting final cut over operation (attempt %d/%d)", i+1, c.dbConfig.MaxRetries)
		c.logger.Infof("Using cutover algorithm: %s", c.algorithm.String())
		switch c.algorithm {
		case RenameUnderLock:
			err = c.algorithmRenameUnderLock(ctx)
		default:
			err = c.algorithmGhost(ctx)
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
	oldName := fmt.Sprintf("_%s_old", c.table.TableName)
	oldQuotedName := fmt.Sprintf("`%s`.`%s`", c.table.SchemaName, oldName)
	renameStatement := fmt.Sprintf("RENAME TABLE %s TO %s, %s TO %s",
		c.table.QuotedName, oldQuotedName,
		c.newTable.QuotedName, c.table.QuotedName,
	)
	return serverLock.ExecUnderLock(ctx, []string{renameStatement})
}

// algorithmGhost is the gh-ost cutover algorithm
// as defined at https://github.com/github/gh-ost/issues/82
func (c *CutOver) algorithmGhost(ctx context.Context) error {
	serverLock, err := dbconn.NewTableLock(ctx, c.db, c.table, false, c.dbConfig, c.logger)
	if err != nil {
		return err
	}
	defer serverLock.Close()

	// Flush all changes exhaustively.
	if err := c.feed.Flush(ctx); err != nil {
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
	// Start the RENAME TABLE trx. This connection is
	// described as C20 in the gh-ost docs.
	trx, connectionID, err := dbconn.BeginStandardTrx(ctx, c.db, nil)
	if err != nil {
		return err
	}
	defer trx.Rollback() //nolint: errcheck
	// Start the rename operation, it's OK it will block inside
	// of this go-routine.
	var wg sync.WaitGroup
	oldQuotedName := fmt.Sprintf("`%s`.`_%s_old`", c.table.SchemaName, c.table.TableName)
	var renameErr error
	wg.Add(1)
	go func() {
		query := fmt.Sprintf("RENAME TABLE %s TO %s, %s TO %s",
			c.table.QuotedName, oldQuotedName,
			c.newTable.QuotedName, c.table.QuotedName)
		_, renameErr = trx.ExecContext(ctx, query)
		wg.Done()
	}()

	// Check that the rename connection is alive and blocked in SHOW PROCESSLIST
	// If this is TRUE then c10 can DROP TABLE tbl_old and then UNLOCK TABLES.
	// If it is not TRUE, it will wait here, since we can't release the server
	// lock until it has started.
	if err := c.checkProcesslistForID(ctx, connectionID); err != nil {
		return err
	}
	// In gh-ost they then DROP the sentry table here from C10.
	// We do not need to do this because we only acquired a table
	// lock on the original table, not on the original + sentry table.
	// From C10 we can release the server lock.
	if err = serverLock.Close(); err != nil {
		return err
	}
	// Wait for the rename to complete from C20
	wg.Wait()
	return renameErr
}

func (c *CutOver) checkProcesslistForID(ctx context.Context, id int) error {
	var state string
	// try up to 10 times. This can be racey
	for i := 0; i < 10; i++ {
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

// Package repl contains binary log subscription functionality.
package repl

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/squareup/spirit/pkg/dbconn"
	"github.com/squareup/spirit/pkg/table"
	"github.com/squareup/spirit/pkg/utils"
)

const (
	binlogTrivialThreshold = 1000
)

type Client struct {
	sync.Mutex
	host     string
	username string
	password string

	binlogChangset      map[string]bool // bool is deleted
	binlogChangsetDelta int64           // a special "fix" for keys that have been popped off.
	binlogStartingPos   *mysql.Position
	canal               *canal.Canal

	changesetRowsCount      int64
	changesetRowsEventCount int64 // eliminated by optimizations

	db *sql.DB // connection to run queries like SHOW MASTER STATUS

	// Infoschema version of table.
	table       *table.TableInfo
	shadowTable *table.TableInfo

	disableKeyAboveWatermarkOptimization bool

	logger log.FieldLogger
}

func NewClient(db *sql.DB, host string, table, shadowTable *table.TableInfo, username, password string, logger log.FieldLogger) *Client {
	return &Client{
		db:             db,
		host:           host,
		table:          table,
		shadowTable:    shadowTable,
		username:       username,
		password:       password,
		binlogChangset: make(map[string]bool),
		logger:         logger,
	}
}

func (c *Client) SetKeyAboveWatermarkOptimization(newVal bool) {
	c.Lock()
	defer c.Unlock()

	c.disableKeyAboveWatermarkOptimization = !newVal
}

func (c *Client) SetStartingPos(pos *mysql.Position) {
	c.Lock()
	defer c.Unlock()

	c.binlogStartingPos = pos
}

func (c *Client) GetBinlogApplyPosition() *mysql.Position {
	c.Lock()
	defer c.Unlock()

	return c.binlogStartingPos
}

func (c *Client) GetDeltaLen() int {
	c.Lock()
	defer c.Unlock()

	return len(c.binlogChangset) + int(c.binlogChangsetDelta)
}

// pksToRowValueConstructor constructs a statement like this:
// DELETE FROM x WHERE (s_i_id,s_w_id) in ((7,10),(1,5));
func (c *Client) pksToRowValueConstructor(d []string) string {
	var pkValues []string
	for _, v := range d {
		pkValues = append(pkValues, utils.UnhashKey(v))
	}
	return strings.Join(pkValues, ",")
}

func (c *Client) getCurrentBinlogPosition() (*mysql.Position, error) {
	var binlogFile, fake string
	var binlogPos uint32
	err := c.db.QueryRow("SHOW MASTER STATUS").Scan(&binlogFile, &binlogPos, &fake, &fake, &fake) //nolint: execinquery
	if err != nil {
		return nil, err
	}
	return &mysql.Position{
		Name: binlogFile,
		Pos:  binlogPos,
	}, nil
}

func (c *Client) Run() (err error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = c.host
	cfg.User = c.username
	cfg.Password = c.password
	cfg.Logger = c.logger
	cfg.IncludeTableRegex = []string{fmt.Sprintf("^%s\\.%s$", c.table.SchemaName, c.table.TableName)}

	c.canal, err = canal.NewCanal(cfg)
	if err != nil {
		return err
	}

	// The handle RowsEvent just writes to the migrators changeset buffer.
	// Which blocks when it needs to be emptied.
	c.canal.SetEventHandler(&MyEventHandler{
		client: c,
	})
	// All we need to do synchronously is get a position before
	// the table migration starts. Then we can start copying data.
	if c.binlogStartingPos == nil {
		c.binlogStartingPos, err = c.getCurrentBinlogPosition()
		if err != nil {
			return fmt.Errorf("failed to get binlog position, is binary logging enabled?")
		}
	} else if c.binlogPositionIsImpossible() {
		// Canal needs to be called as a go routine, so before we do check that the binary log
		// Position is not impossible so we can return a synchronous error.
		return fmt.Errorf("binlog position is impossible, the source may have already purged it")
	}

	// Call start canal as a go routine.
	go c.startCanal()
	return nil
}

func (c *Client) binlogPositionIsImpossible() bool {
	rows, err := c.db.Query("SHOW MASTER LOGS") //nolint: execinquery
	if err != nil {
		return true // if we can't get the logs, its already impossible
	}
	defer rows.Close()

	// Get the number of columns
	cols, err := rows.Columns()
	if err != nil {
		return true
	}
	var logname, size, encrypted string
	for rows.Next() {
		if len(cols) == 3 {
			// MySQL 8.0
			if err := rows.Scan(&logname, &size, &encrypted); err != nil {
				return true
			}
		} else {
			// MySQL 5.7
			if err := rows.Scan(&logname, &size); err != nil {
				return true
			}
		}
		if logname == c.binlogStartingPos.Name {
			return false // We just need presence of the log file for success
		}
	}
	return true
}

// Called as a go routine.
func (c *Client) startCanal() {
	// Start canal as a routine
	c.logger.WithFields(log.Fields{
		"log-file": c.binlogStartingPos.Name,
		"log-pos":  c.binlogStartingPos.Pos,
	}).Debug("starting binary log subscription")
	if err := c.canal.RunFrom(*c.binlogStartingPos); err != nil {
		// Canal has failed! In future we might be able to reconnect and resume
		// if canal does not do so itself. For now, we just fail the migration
		// since we can resume from checkpoint anyway.
		c.logger.WithFields(log.Fields{
			"error": err,
		}).Error("canal has failed!")
		panic("canal has failed")
	}
}

func (c *Client) Close() {
	if c.canal != nil {
		c.canal.Close()
	}
}

func (c *Client) Flush(ctx context.Context) error {
	c.Lock()
	setToFlush := c.binlogChangset
	c.binlogChangset = make(map[string]bool) // set new value
	c.Unlock()                               // unlock immediately so others can write to the changeset
	// The changeset delta is because the status output is based on len(binlogChangeset)
	// which just got reset to zero. We need some way to communicate roughly in status output
	// there is other pending work while this func is running. We'll reset the delta
	// to zero when this func exits.
	atomic.StoreInt64(&c.binlogChangsetDelta, int64(len(setToFlush)))

	defer func() {
		atomic.AddInt64(&c.changesetRowsCount, int64(len(setToFlush)))
		atomic.StoreInt64(&c.binlogChangsetDelta, int64(0)) // reset the delta
	}()

	// We must now apply the changeset setToFlush to the shadow table.
	var deleteKeys []string
	var replaceKeys []string
	var i int
	for key, isDelete := range setToFlush {
		i++
		if isDelete {
			deleteKeys = append(deleteKeys, key)
		} else {
			replaceKeys = append(replaceKeys, key)
		}
		if (i % 10000) == 0 {
			if err := c.doFlush(ctx, &deleteKeys, &replaceKeys); err != nil {
				return err
			}
			atomic.AddInt64(&c.binlogChangsetDelta, -10000)
		}
	}
	err := c.doFlush(ctx, &deleteKeys, &replaceKeys)
	return err
}

// doFlush is called by Flush() to apply the changeset to the shadow table.
// It runs the actual SQL statements using DELETE FROM and REPLACE INTO syntax.
// This is called under a mutex from Flush().
func (c *Client) doFlush(ctx context.Context, deleteKeys, replaceKeys *[]string) error {
	var deleteStmt, replaceStmt string
	if len(*deleteKeys) > 0 {
		deleteStmt = fmt.Sprintf("DELETE FROM %s WHERE (%s) IN (%s)",
			c.shadowTable.QuotedName(),
			strings.Join(c.shadowTable.PrimaryKey, ","),
			c.pksToRowValueConstructor(*deleteKeys),
		)
	}
	if len(*replaceKeys) > 0 {
		replaceStmt = fmt.Sprintf("REPLACE INTO %s (%s) SELECT %s FROM %s WHERE (%s) IN (%s)",
			c.shadowTable.QuotedName(),
			utils.IntersectColumns(c.table, c.shadowTable, false),
			utils.IntersectColumns(c.table, c.shadowTable, false),
			c.table.QuotedName(),
			strings.Join(c.shadowTable.PrimaryKey, ","),
			c.pksToRowValueConstructor(*replaceKeys),
		)
	}
	// This will start + commit the transaction
	// And retry it if there are deadlocks etc.
	if _, err := dbconn.RetryableTransaction(ctx, c.db, false, deleteStmt, replaceStmt); err != nil {
		return err
	}
	// Reset the deleteKeys and replaceKeys so they can be used again.
	*deleteKeys = []string{}
	*replaceKeys = []string{}
	return nil
}

func (c *Client) FlushUntilTrivial(ctx context.Context) error {
	c.logger.Info("starting to flush changeset")

	for {
		// Repeat in a loop until the changeset length is trivial
		if err := c.Flush(ctx); err != nil {
			return err
		}
		// Wait for canal to catch up before determining if the changeset
		// length is considered trivial.
		if err := c.BlockWait(); err != nil {
			return err
		}

		c.Lock()
		changetSetLen := len(c.binlogChangset)
		c.Unlock()
		if changetSetLen < binlogTrivialThreshold {
			break
		}
	}
	return nil
}

func (c *Client) BlockWait() error {
	targetPos, err := c.canal.GetMasterPos() // what the server is at.
	if err != nil {
		return err
	}
	return c.canal.WaitUntilPos(targetPos, 24*14*time.Hour)
}

func (c *Client) keyHasChanged(key []interface{}, deleted bool) {
	c.Lock()
	defer c.Unlock()

	c.binlogChangset[utils.HashKey(key)] = deleted
}

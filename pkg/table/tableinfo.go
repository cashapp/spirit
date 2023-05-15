// Package table contains some common utilities for working with tables
// such as a 'Chunker' feature.
package table

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/siddontang/loggers"
)

const (
	trivialChunkerThreshold      = 1000
	lastChunkStatisticsThreshold = 10 * time.Second
)

var (
	ErrTableIsRead       = errors.New("table is read")
	ErrTableNotOpen      = errors.New("please call Open() first")
	ErrUnsupportedPKType = errors.New("unsupported primary key type")
)

type TableInfo struct {
	sync.Mutex
	db                    *sql.DB
	EstimatedRows         uint64
	SchemaName            string
	TableName             string
	QuotedName            string
	PrimaryKey            []string
	Columns               []string
	pkMySQLTp             []string  // the MySQL type of pk
	pkDatumTp             []datumTp // the datum type of pk
	PrimaryKeyIsAutoInc   bool      // if pk[0] is an auto_increment column
	minValue              datum     // known minValue of pk[0] (using type of PK[0])
	maxValue              datum     // known maxValue of pk[0] (using type of PK[0])
	isClosed              bool      // if this tableInfo is closed.
	statisticsLastUpdated time.Time
	statisticsLock        sync.Mutex
}

func NewTableInfo(db *sql.DB, schema, table string) *TableInfo {
	return &TableInfo{
		db:         db,
		SchemaName: schema,
		TableName:  table,
		QuotedName: fmt.Sprintf("`%s`.`%s`", schema, table),
	}
}

// isCompatibleWithChunker checks that the PRIMARY KEY type is compatible.
// We currently repeat this check in Open().
// Important! we can support non-integer primary keys, but they
// must be binary comparable! Otherwise features like the deltaMap
// won't work correctly! Collations also affect chunking behavior in possibly
// unsafe ways!
func (t *TableInfo) isCompatibleWithChunker() error {
	if mySQLTypeToDatumTp(t.pkMySQLTp[0]) == unknownType {
		return ErrUnsupportedPKType
	}
	return nil
}

// PrimaryKeyValues helps extract the PRIMARY KEY from a row image.
// It uses our knowledge of the ordinal position of columns to find the
// position of primary key columns (there might be more than one).
func (t *TableInfo) PrimaryKeyValues(row interface{}) []interface{} {
	var pkCols []interface{}
	for _, pCol := range t.PrimaryKey {
		for i, col := range t.Columns {
			if col == pCol {
				pkCols = append(pkCols, row.([]interface{})[i])
			}
		}
	}
	return pkCols
}

// SetInfo reads from MySQL metadata (usually infoschema) and sets the values in TableInfo.
func (t *TableInfo) SetInfo(ctx context.Context) error {
	if err := t.setRowEstimate(ctx); err != nil {
		return err
	}
	if err := t.setColumns(ctx); err != nil {
		return err
	}
	if err := t.setPrimaryKey(ctx); err != nil {
		return err
	}
	// Check primary key is memory comparable.
	// In future this may become optional, since it's not a chunker requirement,
	// but a requirement for the deltaMap.
	if err := t.checkPrimaryKeyIsMemoryComparable(ctx); err != nil {
		return err
	}
	return t.setMinMax(ctx)
}

// setRowEstimate is a separate function so it can be repeated continuously
// Since if a schema migration takes 14 days, it could change.
func (t *TableInfo) setRowEstimate(ctx context.Context) error {
	_, err := t.db.ExecContext(ctx, "ANALYZE TABLE "+t.QuotedName)
	if err != nil {
		return err
	}
	err = t.db.QueryRowContext(ctx, "SELECT IFNULL(table_rows,0) FROM information_schema.tables WHERE table_schema=? AND table_name=?", t.SchemaName, t.TableName).Scan(&t.EstimatedRows)
	if err != nil {
		return err
	}
	return nil
}

func (t *TableInfo) setColumns(ctx context.Context) error {
	rows, err := t.db.QueryContext(ctx, "SELECT column_name FROM information_schema.columns WHERE table_schema=? AND table_name=? ORDER BY ORDINAL_POSITION",
		t.SchemaName,
		t.TableName,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	t.Columns = []string{}
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return err
		}
		t.Columns = append(t.Columns, col)
	}
	return nil
}

// setPrimaryKey sets the primary key and also the primary key type.
// A primary key can contain multiple columns.
func (t *TableInfo) setPrimaryKey(ctx context.Context) error {
	rows, err := t.db.QueryContext(ctx, "SELECT column_name FROM information_schema.key_column_usage WHERE table_schema=? and table_name=? and constraint_name='PRIMARY' ORDER BY ORDINAL_POSITION",
		t.SchemaName,
		t.TableName,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	t.PrimaryKey = []string{}
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return err
		}
		t.PrimaryKey = append(t.PrimaryKey, col)
	}
	if len(t.PrimaryKey) == 0 {
		return errors.New("no primary key found (not supported)")
	}
	for i, col := range t.PrimaryKey {
		// Get primary key type and auto_inc info.
		query := "SELECT column_type, extra FROM information_schema.columns WHERE table_schema=? AND table_name=? and column_name=?"
		var extra, pkType string
		err = t.db.QueryRowContext(ctx, query, t.SchemaName, t.TableName, col).Scan(&pkType, &extra)
		if err != nil {
			return err
		}
		pkType = removeWidth(pkType)
		t.pkMySQLTp = append(t.pkMySQLTp, pkType)
		t.pkDatumTp = append(t.pkDatumTp, mySQLTypeToDatumTp(pkType))
		if i == 0 {
			t.PrimaryKeyIsAutoInc = (extra == "auto_increment")
		}
	}
	return nil
}

func (t *TableInfo) checkPrimaryKeyIsMemoryComparable(ctx context.Context) error {
	for _, col := range t.PrimaryKey {
		var colType string
		query := "SELECT column_type FROM information_schema.columns WHERE table_schema=? AND table_name=? and column_name=?"
		err := t.db.QueryRowContext(ctx, query, t.SchemaName, t.TableName, col).Scan(&colType)
		if err != nil {
			return err
		}
		if mySQLTypeToDatumTp(colType) == unknownType {
			return fmt.Errorf("primary key contains %s which is not memory comparable", colType)
		}
	}
	return nil
}

// setMinMax is a separate function so it can be repeated continuously
// Since if a schema migration takes 14 days, it could change.
func (t *TableInfo) setMinMax(ctx context.Context) error {
	if t.pkDatumTp[0] == binaryType {
		return nil // we don't min/max binary types for now.
	}
	query := fmt.Sprintf("SELECT IFNULL(min(%s),'0'), IFNULL(max(%s),'0') FROM %s", t.PrimaryKey[0], t.PrimaryKey[0], t.QuotedName)
	var min, max string
	err := t.db.QueryRowContext(ctx, query).Scan(&min, &max)
	if err != nil {
		return err
	}

	t.minValue, err = newDatumFromMySQL(min, t.pkMySQLTp[0])
	if err != nil {
		return err
	}
	t.maxValue, err = newDatumFromMySQL(max, t.pkMySQLTp[0])
	if err != nil {
		return err
	}
	return nil
}

// Close closes the tableInfo and stops the goroutine that updates the table statistics.
func (t *TableInfo) Close() error {
	t.isClosed = true
	return nil
}

// AutoUpdateStatistics runs a loop that updates the table statistics every interval.
// This will continue until Close() is called on the tableInfo.
func (t *TableInfo) AutoUpdateStatistics(ctx context.Context, interval time.Duration, logger loggers.Advanced) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		if t.isClosed {
			return
		}
		if err := t.updateTableStatistics(ctx); err != nil {
			logger.Errorf("error updating table statistics: %v", err)
		}
		logger.Infof("table statistics updated: estimated-rows=%d pk[0].max-value=%v", t.EstimatedRows, t.MaxValue())
	}
}

// statisticsNeedUpdating returns true if the statistics are considered order than a threshold.
// this is useful for the chunker to synchronously check as it approaches the end of the table.
func (t *TableInfo) statisticsNeedUpdating() bool {
	threshold := time.Now().Add(-lastChunkStatisticsThreshold)
	return t.statisticsLastUpdated.Before(threshold)
}

// updateTableStatistics recalculates the min/max and row estimate.
func (t *TableInfo) updateTableStatistics(ctx context.Context) error {
	t.statisticsLock.Lock()
	defer t.statisticsLock.Unlock()
	err := t.setMinMax(ctx)
	if err != nil {
		return err
	}
	err = t.setRowEstimate(ctx)
	if err != nil {
		return err
	}
	t.statisticsLastUpdated = time.Now()
	return nil
}

// MaxValue as a string
func (t *TableInfo) MaxValue() string {
	return fmt.Sprintf("%v", t.maxValue)
}

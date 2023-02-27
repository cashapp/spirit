// Package table contains some common utilities for working with tables
// such as a 'Chunker' feature.
package table

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
)

type simplifiedKeyType int

const (
	unknownType simplifiedKeyType = iota
	signedType
	unsignedType
	binaryType

	trivialChunkerThreshold = 1000
)

var (
	ErrTableIsRead       = errors.New("table is read")
	ErrTableNotOpen      = errors.New("please call Open() first")
	ErrUnsupportedPKType = errors.New("unsupported primary key type")
)

type TableInfo struct {
	sync.Mutex
	db                  *sql.DB
	EstimatedRows       uint64
	SchemaName          string
	TableName           string
	PrimaryKey          []string
	Columns             []string
	primaryKeyType      string // the MySQL type.
	primaryKeyIsAutoInc bool
	minValue            interface{} // known minValue of pk[0] (using type of PK)
	maxValue            interface{} // known maxValue of pk[0] (using type of PK)
}

func NewTableInfo(db *sql.DB, schema, table string) *TableInfo {
	return &TableInfo{
		db:         db,
		SchemaName: schema,
		TableName:  table,
	}
}

// isCompatibleWithChunker checks that the PRIMARY KEY type is compatible.
// We currently repeat this check in Open().
// Important! we can support non-integer primary keys, but they
// must be binary comparable! Otherwise features like the deltaMap
// won't work correctly! Collations also affect chunking behavior in possibly
// unsafe ways!
func (t *TableInfo) isCompatibleWithChunker() error {
	if mySQLTypeToSimplifiedKeyType(t.primaryKeyType) == unknownType {
		return ErrUnsupportedPKType
	}
	return nil
}

func (t *TableInfo) QuotedName() string {
	return fmt.Sprintf("`%s`.`%s`", t.SchemaName, t.TableName)
}

// ExtractPrimaryKeyFromRowImage helps extract the PRIMARY KEY from a row image.
// It uses our knowledge of the ordinal position of columns to find the
// position of primary key columns (there might be more than one).
func (t *TableInfo) ExtractPrimaryKeyFromRowImage(row interface{}) []interface{} {
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
	_, err := t.db.ExecContext(ctx, "ANALYZE TABLE "+t.QuotedName())
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
	// Get primary key type and auto_inc info.
	query := "SELECT column_type, extra FROM information_schema.columns WHERE table_schema=? AND table_name=? and column_name=?"
	var extra string
	err = t.db.QueryRowContext(ctx, query, t.SchemaName, t.TableName, t.PrimaryKey[0]).Scan(&t.primaryKeyType, &extra)
	if err != nil {
		return err
	}
	t.primaryKeyType = removeWidth(t.primaryKeyType)
	t.primaryKeyIsAutoInc = (extra == "auto_increment")
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
		if mySQLTypeToSimplifiedKeyType(colType) == unknownType {
			return fmt.Errorf("primary key contains %s which is not memory comparable", colType)
		}
	}
	return nil
}

// setMinMax is a separate function so it can be repeated continuously
// Since if a schema migration takes 14 days, it could change.
func (t *TableInfo) setMinMax(ctx context.Context) error {
	// We can't scan into interface{} because the types will be wonky.
	// See: https://github.com/go-sql-driver/mysql/issues/366
	// This is a workaround which is a bit ugly, but type preserving.
	query := fmt.Sprintf("SELECT min(%s), max(%s) FROM %s", t.PrimaryKey[0], t.PrimaryKey[0], t.QuotedName())
	var err error
	switch mySQLTypeToSimplifiedKeyType(t.primaryKeyType) {
	case signedType:
		var min, max sql.NullInt64
		err = t.db.QueryRowContext(ctx, query).Scan(&min, &max)
		if err != nil {
			return err
		}
		// If min/max valid it means there are rows in the table.
		if min.Valid && max.Valid {
			t.minValue, t.maxValue = min.Int64, max.Int64
		}
	case unsignedType:
		query = fmt.Sprintf("SELECT IFNULL(min(%s),0), IFNULL(max(%s),0) FROM %s", t.PrimaryKey[0], t.PrimaryKey[0], t.QuotedName())
		var min, max uint64 // there is no sql.NullUint64
		err = t.db.QueryRowContext(ctx, query).Scan(&min, &max)
		if err != nil {
			return err
		}
		if max > 0 { // check for a maxVal, minval=0 could be valid.
			t.minValue, t.maxValue = min, max
		}
	case binaryType:
		var min, max sql.NullString
		err = t.db.QueryRowContext(ctx, query).Scan(&min, &max)
		if err != nil {
			return err
		}
		// If min/max valid it means there are rows in the table.
		if min.Valid && max.Valid {
			t.minValue, t.maxValue = min.String, max.String
		}
	default:
		return ErrUnsupportedPKType
	}
	return err
}

// UpdateTableStatistics recalculates the min/max and row estimate.
// It is exported so it can be used by the caller to continuously update the table stats.
func (t *TableInfo) UpdateTableStatistics(ctx context.Context) error {
	err := t.setMinMax(ctx)
	if err != nil {
		return err
	}
	return t.setRowEstimate(ctx)
}

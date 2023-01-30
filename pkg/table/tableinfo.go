// Package table contains some common utilities for working with tables
// such as a 'Chunker' feature.
package table

import (
	"database/sql"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
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
	ErrTableIsRead       = fmt.Errorf("table is read")
	ErrTableNotOpen      = fmt.Errorf("please call Open() first")
	ErrUnsupportedPKType = fmt.Errorf("unsupported primary key type")
)

type TableInfo struct {
	sync.Mutex
	EstimatedRows       uint64
	SchemaName          string
	TableName           string
	PrimaryKey          []string
	Columns             []string
	primaryKeyType      string // the MySQL type.
	primaryKeyIsAutoInc bool
	minValue            interface{} // known minValue of pk[0] (using type of PK)
	maxValue            interface{} // known maxValue of pk[0] (using type of PK)
	Chunker             Chunker

	logger log.FieldLogger
}

func NewTableInfo(schema, table string) *TableInfo {
	return &TableInfo{
		SchemaName: schema,
		TableName:  table,
		logger:     log.New(),
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

func (t *TableInfo) AttachChunker(chunkerTargetMs int64, disableTrivialChunker bool, logger log.FieldLogger) error {
	// If the row count is low we just attach
	// "the trivial chunker" (i.e. the chunker base)
	// which will return everything as one chunk.
	if t.EstimatedRows < trivialChunkerThreshold && !disableTrivialChunker {
		t.Chunker = &chunkerBase{Ti: t}
		return nil
	}
	if chunkerTargetMs == 0 {
		chunkerTargetMs = 100
	}
	if logger != nil {
		t.logger = logger
	}
	chunkerBase := &chunkerBase{
		Ti:              t,
		chunkSize:       uint64(1000),    // later this might become configurable.
		ChunkerTargetMs: chunkerTargetMs, // this is the main chunker target.
	}
	switch mySQLTypeToSimplifiedKeyType(t.primaryKeyType) {
	case signedType:
		t.Chunker = &chunkerSigned{
			chunkerBase: chunkerBase,
		}
	case unsignedType:
		t.Chunker = &chunkerUnsigned{
			chunkerBase: chunkerBase,
		}
	case binaryType:
		t.Chunker = &chunkerBinary{
			chunkerBase: chunkerBase,
		}
	default:
		return ErrUnsupportedPKType
	}
	return nil
}

// RunDiscovery requires a database connection, which means it can't easily be mocked in unit tests.
// Where possible discovery funcs should update the TableInfo struct directly, and not be called by
// internal functions. This allows the TableInfo to be mocked in tests.
func (t *TableInfo) RunDiscovery(db *sql.DB) error {
	// Discover row estimate
	if err := t.discoverRowEstimate(db); err != nil {
		return fmt.Errorf("err: are you sure table %s exists?", t.QuotedName())
	}
	// Discover columns
	rows, err := db.Query("SELECT column_name FROM information_schema.columns WHERE table_schema=? AND table_name=? ORDER BY ORDINAL_POSITION",
		t.SchemaName,
		t.TableName,
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return err
		}
		t.Columns = append(t.Columns, col)
	}
	rows.Close()

	// Discover primary key
	rows, err = db.Query("SELECT column_name FROM information_schema.key_column_usage WHERE table_schema=? and table_name=? and constraint_name='PRIMARY' ORDER BY ORDINAL_POSITION",
		t.SchemaName,
		t.TableName,
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return err
		}
		t.PrimaryKey = append(t.PrimaryKey, col)
	}
	rows.Close()
	if len(t.PrimaryKey) == 0 {
		return fmt.Errorf("no primary key found (not supported)")
	}
	// Get primary key type and auto_inc info.
	query := "SELECT column_type, extra FROM information_schema.columns WHERE table_schema=? AND table_name=? and column_name=?"
	var extra string
	err = db.QueryRow(query, t.SchemaName, t.TableName, t.PrimaryKey[0]).Scan(&t.primaryKeyType, &extra)
	if err != nil {
		return err
	}
	t.primaryKeyIsAutoInc = (extra == "auto_increment")
	return t.discoverMinMax(db)
}

// discoverRowEstimate is a separate function so it can be repeated continuously
// Since if a schema migration takes 14 days, it could change.
func (t *TableInfo) discoverRowEstimate(db *sql.DB) error {
	err := db.QueryRow("SELECT IFNULL(table_rows,0) FROM information_schema.tables WHERE table_schema=? AND table_name=?", t.SchemaName, t.TableName).Scan(&t.EstimatedRows)
	if err != nil {
		return fmt.Errorf("err: are you sure table %s exists?", t.QuotedName())
	}
	return nil
}

// discoverMinMax is a separate function so it can be repeated continuously
// Since if a schema migration takes 14 days, it could change.
func (t *TableInfo) discoverMinMax(db *sql.DB) error {
	// We can't scan into interface{} because the types will be wonky.
	// See: https://github.com/go-sql-driver/mysql/issues/366
	// This is a workaround which is a bit ugly, but type preserving.
	query := fmt.Sprintf("SELECT min(%s), max(%s) FROM %s", t.PrimaryKey[0], t.PrimaryKey[0], t.QuotedName())
	var err error
	switch mySQLTypeToSimplifiedKeyType(t.primaryKeyType) {
	case signedType:
		var min, max sql.NullInt64
		err = db.QueryRow(query).Scan(&min, &max)
		if err == nil && min.Valid && max.Valid {
			t.minValue, t.maxValue = min.Int64, max.Int64
		}
	case unsignedType:
		query = fmt.Sprintf("SELECT IFNULL(min(%s),0), IFNULL(max(%s),0) FROM %s", t.PrimaryKey[0], t.PrimaryKey[0], t.QuotedName())
		var min, max uint64 // there is no sql.NullUint64
		err = db.QueryRow(query).Scan(&min, &max)
		if err == nil && max > 0 { // check for a maxVal, minval=0 could be valid.
			t.minValue, t.maxValue = min, max
		}
	case binaryType:
		var min, max sql.NullString
		err = db.QueryRow(query).Scan(&min, &max)
		if err == nil && min.Valid && max.Valid {
			t.minValue, t.maxValue = min.String, max.String
		}
	default:
		return ErrUnsupportedPKType
	}
	return err
}

// UpdateTableStatistics recalculates the min/max and row estimate.
// It is exported so it can be used by the caller to continuously update the table stats.
func (t *TableInfo) UpdateTableStatistics(db *sql.DB) error {
	err := t.discoverMinMax(db)
	if err != nil {
		return err
	}
	return t.discoverRowEstimate(db)
}

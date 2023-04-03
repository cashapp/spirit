package migration

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"

	"github.com/squareup/spirit/pkg/table"
)

// IsCompatible checks if a migration is compatible with this program.
// This can be used by automation to fallback to gh-ost if it's not.
func IsCompatible(ctx context.Context, migration *Migration) bool {
	m, err := NewRunner(migration)
	if err != nil {
		return false
	}
	m.db, err = sql.Open("mysql", m.dsn())
	if err != nil {
		return false
	}

	// Get Table Info
	m.table = table.NewTableInfo(m.db, m.schemaName, m.tableName)
	if err := m.table.SetInfo(ctx); err != nil {
		return false
	}
	// Check that we can get a chunker.
	if _, err := table.NewChunker(m.table, m.optTargetChunkTime, m.optDisableTrivialChunker, m.logger); err != nil {
		return false
	}
	return true
}

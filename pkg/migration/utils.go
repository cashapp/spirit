package migration

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"

	"github.com/squareup/spirit/pkg/table"
)

// IsCompatible checks if a migration is compatible with this program.
// This can be used by automation to fallback to gh-ost if it's not.
func IsCompatible(migration *Migration) bool {
	m, err := NewMigrationRunner(migration)
	if err != nil {
		return false
	}
	m.db, err = sql.Open("mysql", m.dsn())
	if err != nil {
		return false
	}

	// Get Table Info
	m.table = table.NewTableInfo(m.schemaName, m.tableName)
	if err := m.table.RunDiscovery(m.db); err != nil {
		return false
	}
	// Attach the correct chunker.
	if err := m.table.AttachChunker(m.optTargetChunkMs, m.optDisableTrivialChunker, m.logger); err != nil {
		return false
	}
	return true
}

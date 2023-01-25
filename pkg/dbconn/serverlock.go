package dbconn

import (
	"context"
	"database/sql"

	"github.com/squareup/spirit/pkg/table"
)

type ServerLock struct {
	table   *table.TableInfo
	lockTxn *sql.Tx
}

// NewServerLock creates a new database-server wide lock
// i.e. LOCK TABLES .. READ
func NewServerLock(ctx context.Context, db *sql.DB, table *table.TableInfo) (*ServerLock, error) {
	lockTxn, _ := db.BeginTx(ctx, nil)
	_, err := lockTxn.Exec("LOCK TABLES " + table.QuotedName() + " READ")
	if err != nil {
		return nil, err
	}
	return &ServerLock{
		table:   table,
		lockTxn: lockTxn,
	}, nil
}

// Close closes the database-server wide lock.
func (s *ServerLock) Close() error {
	_, err := s.lockTxn.Exec("UNLOCK TABLES")
	if err != nil {
		return err
	}
	err = s.lockTxn.Rollback()
	if err != nil {
		return err
	}
	return nil
}

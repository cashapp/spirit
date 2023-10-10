package checksum

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/siddontang/loggers"
	"github.com/squareup/spirit/pkg/dbconn"
	"github.com/squareup/spirit/pkg/table"
	"github.com/squareup/spirit/pkg/utils"
)

// The replace resolver re-copies a chunk from the source table.
// Because REPLACE does not handle deletes, we have to DELETE
// and then INSERT the chunk in a transactional way.

type ReplaceResolver struct {
	db     *sql.DB
	source *table.TableInfo
	target *table.TableInfo
	logger loggers.Advanced
}

var _ Resolver = &ReplaceResolver{}

func (r *ReplaceResolver) Open(db *sql.DB, source, target *table.TableInfo, logger loggers.Advanced) error {
	r.db = db
	r.source = source
	r.target = target
	r.logger = logger
	return nil
}

// Resolve deletes all the data in the target and recopies it from the source.
// It should mean that all the data is now consistent, but we retry the checksum
// again if it failed.
func (r *ReplaceResolver) Resolve(trx *sql.Tx, chunk *table.Chunk, sourceChecksum int64, targetChecksum int64) error {
	r.logger.Errorf("Recopying chunk for failed checksum on chunk %s: source %d != target %d", chunk.String(), sourceChecksum, targetChecksum)
	deleteStmt := "DELETE FROM " + r.target.QuotedName + " WHERE " + chunk.String()
	replaceStmt := fmt.Sprintf("REPLACE INTO %s (%s) SELECT %s FROM %s WHERE %s",
		r.target.QuotedName,
		utils.IntersectColumns(r.source, r.target),
		utils.IntersectColumns(r.source, r.target),
		r.source.QuotedName,
		chunk.String(),
	)
	_, err := dbconn.RetryableTransaction(context.TODO(), r.db, false, dbconn.NewDBConfig(), deleteStmt, replaceStmt)
	return err
}

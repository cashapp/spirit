package checksum

import (
	"database/sql"

	"github.com/siddontang/loggers"
	"github.com/squareup/spirit/pkg/table"
)

type Resolver interface {
	Open(db *sql.DB, source *table.TableInfo, target *table.TableInfo, logger loggers.Advanced) error
	Resolve(trx *sql.Tx, chunk *table.Chunk, sourceChecksum int64, targetChecksum int64) error
}

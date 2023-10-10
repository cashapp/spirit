package checksum

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/siddontang/loggers"
	"github.com/squareup/spirit/pkg/table"
)

// The noop resolver does nothing to resolve.
// It just inspects the failed chunk in more detail to isolate
// exactly where the mismatch is.

type NoopResolver struct {
	source *table.TableInfo
	target *table.TableInfo
	logger loggers.Advanced
}

var _ Resolver = &NoopResolver{}

func (r *NoopResolver) Open(_ *sql.DB, source, target *table.TableInfo, logger loggers.Advanced) error {
	r.source = source
	r.target = target
	r.logger = logger
	return nil
}

// Resolve in this resolver finds the exact differences between the source and target checksums.
// But it doesn't actually resolve them. Instead it returns an error summarizing the differences.
// This is the default strategy used for resolving checksum errors, because checksum errors
// are not supposed to happen.
func (r *NoopResolver) Resolve(trx *sql.Tx, chunk *table.Chunk, sourceChecksum int64, targetChecksum int64) error {
	r.logger.Warnf("checksum mismatch for chunk %s: source %d != target %d", chunk.String(), sourceChecksum, targetChecksum)
	sourceSubquery := fmt.Sprintf("SELECT CRC32(CONCAT(%s)) as row_checksum, %s FROM %s WHERE %s",
		r.intersectColumns(),
		strings.Join(r.source.KeyColumns, ", "),
		r.source.QuotedName,
		chunk.String(),
	)
	targetSubquery := fmt.Sprintf("SELECT CRC32(CONCAT(%s)) as row_checksum, %s FROM %s WHERE %s",
		r.intersectColumns(),
		strings.Join(r.target.KeyColumns, ", "),
		r.target.QuotedName,
		chunk.String(),
	)
	// In MySQL 8.0 we could do this as a CTE, but because we kinda support
	// MySQL 5.7 we have to do it as a subquery. It should be a small amount of data
	// because its one chunk. Note: we technically have to do this twice, since
	// extra rows could exist on either side and we can't rely on FULL OUTER JOIN existing.
	stmt := fmt.Sprintf(`SELECT source.row_checksum as source_row_checksum, target.row_checksum as target_row_checksum, CONCAT_WS(",", %s) as pk FROM (%s) AS source LEFT JOIN (%s) AS target USING (%s) WHERE source.row_checksum != target.row_checksum OR target.row_checksum IS NULL
UNION
SELECT source.row_checksum as source_row_checksum, target.row_checksum as target_row_checksum, CONCAT_WS(",", %s) as pk FROM (%s) AS source RIGHT JOIN (%s) AS target USING (%s) WHERE source.row_checksum != target.row_checksum OR source.row_checksum IS NULL
	`,
		strings.Join(r.source.KeyColumns, ", "),
		sourceSubquery,
		targetSubquery,
		strings.Join(r.source.KeyColumns, ", "),
		strings.Join(r.source.KeyColumns, ", "),
		sourceSubquery,
		targetSubquery,
		strings.Join(r.source.KeyColumns, ", "),
	)
	res, err := trx.Query(stmt)
	if err != nil {
		return err // can not debug issue
	}
	defer res.Close()
	var sourceRowChecksum, targetRowChecksum sql.NullString
	var pk string
	for res.Next() {
		err = res.Scan(&sourceRowChecksum, &targetRowChecksum, &pk)
		if err != nil {
			return err // can not debug issue
		}
		if sourceRowChecksum.Valid && !targetRowChecksum.Valid {
			r.logger.Warnf("row does not exist in target for pk: %s", pk)
		} else if !sourceRowChecksum.Valid && targetRowChecksum.Valid {
			r.logger.Warnf("row does not exist in source for pk: %s", pk)
		} else {
			r.logger.Warnf("row checksum mismatch for pk: %s: source %s != target %s", pk, sourceRowChecksum.String, targetRowChecksum.String)
		}
	}
	return errors.New("checksum mismatch")
}

func (r *NoopResolver) intersectColumns() string {
	var intersection []string
	for _, col := range r.source.Columns {
		for _, col2 := range r.target.Columns {
			if col == col2 {
				// Column exists in both, so we add intersection wrapped in
				// IFNULL, ISNULL and CAST.
				intersection = append(intersection, "IFNULL("+r.target.WrapCastType(col)+",''), ISNULL(`"+col+"`)")
			}
		}
	}
	return strings.Join(intersection, ", ")
}

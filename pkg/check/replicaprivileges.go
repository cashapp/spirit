package check

import (
	"context"
	"database/sql"

	"github.com/siddontang/loggers"
	"github.com/squareup/spirit/pkg/throttler"
)

func init() {
	registerCheck("replica", replicaPrivilegeCheck, ScopeReplicaPreflight)
}

// Check that there is permission to run perfschema queries for replication (8.0)
// or SHOW SLAVE STATUS (5.7).
func replicaPrivilegeCheck(ctx context.Context, db *sql.DB, logger loggers.Advanced) error {
	var version string
	if err := db.QueryRow("select substr(version(), 1, 1)").Scan(&version); err != nil {
		return err //  can not get version
	}
	lagQuery := `SHOW SLAVE STATUS`
	if version == "8" {
		lagQuery = throttler.MySQL8LagQuery
	}
	var output string
	err := db.QueryRowContext(ctx, lagQuery).Scan(&output) //nolint: execinquery
	return err
}

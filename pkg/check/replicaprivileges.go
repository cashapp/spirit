package check

import (
	"context"

	"github.com/siddontang/loggers"
	"github.com/squareup/spirit/pkg/throttler"
)

func init() {
	registerCheck("replica", replicaPrivilegeCheck, ScopePreflight)
}

// Check that there is permission to run perfschema queries for replication (8.0)
// or SHOW SLAVE STATUS (5.7).
func replicaPrivilegeCheck(ctx context.Context, r Resources, logger loggers.Advanced) error {
	if r.Replica == nil {
		return nil // The user is not using the replica DSN feature.
	}
	var version string
	if err := r.Replica.QueryRow("select substr(version(), 1, 1)").Scan(&version); err != nil {
		return err //  can not get version
	}
	lagQuery := `SHOW SLAVE STATUS`
	if version == "8" {
		lagQuery = throttler.MySQL8LagQuery
	}
	rows, err := r.Replica.Query(lagQuery) //nolint: execinquery
	if err != nil {
		return err
	}
	defer rows.Close()
	_, err = scanToMap(rows)
	return err
}

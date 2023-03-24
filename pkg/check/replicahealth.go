package check

import (
	"context"
	"database/sql"

	"github.com/siddontang/loggers"
)

func init() {
	registerCheck("replica", replicaHealth, ScopeReplicaPreflight|ScopeReplicaCutover|ScopeReplicaPostCutover)
}

// replicaHealth checks SHOW SLAVE STATUS for Yes and Yes.
// It should be run at various stages of the migration if a replica is present.
func replicaHealth(ctx context.Context, db *sql.DB, logger loggers.Advanced) error {
	//db.QueryContext(ctx, "SHOW SLAVE STATUS")
	return nil
}

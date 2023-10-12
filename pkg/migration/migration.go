// Package migration contains the logic for running online schema changes.
package migration

import (
	"context"
	"time"

	"github.com/cashapp/spirit/pkg/check"
)

type Migration struct {
	Host                 string        `name:"host" help:"Hostname" optional:"" default:"127.0.0.1:3306"`
	Username             string        `name:"username" help:"User" optional:"" default:"msandbox"`
	Password             string        `name:"password" help:"Password" optional:"" default:"msandbox"`
	Database             string        `name:"database" help:"Database" optional:"" default:"test"`
	Table                string        `name:"table" help:"Table" optional:"" default:"stock"`
	Alter                string        `name:"alter" help:"The alter statement to run on the table" optional:"" default:"engine=innodb"`
	Threads              int           `name:"threads" help:"Number of concurrent threads for copy and checksum tasks" optional:"" default:"4"`
	TargetChunkTime      time.Duration `name:"target-chunk-time" help:"The target copy time for each chunk" optional:"" default:"500ms"`
	AttemptInplaceDDL    bool          `name:"attempt-inplace-ddl" help:"Attempt inplace DDL (only safe without replicas or with Aurora Global)" optional:"" default:"false"`
	Checksum             bool          `name:"checksum" help:"Checksum new table before final cut-over" optional:"" default:"true"`
	ReplicaDSN           string        `name:"replica-dsn" help:"A DSN for a replica which (if specified) will be used for lag checking." optional:""`
	ReplicaMaxLag        time.Duration `name:"replica-max-lag" help:"The maximum lag allowed on the replica before the migration throttles." optional:"" default:"120s"`
	LockWaitTimeout      time.Duration `name:"lock-wait-timeout" help:"The DDL lock_wait_timeout required for checksum and cutover" optional:"" default:"30s"`
	SkipPreRunChecks     bool          `name:"i-understand-mysql57-is-not-supported" hidden:"" default:"false"`
	SkipDropAfterCutover bool          `name:"skip-drop-after-cutover" help:"Keep old table after completing cutover" optional:"" default:"false"`
}

func (m *Migration) Run() error {
	migration, err := NewRunner(m)
	if err != nil {
		return err
	}
	defer migration.Close()
	if !m.SkipPreRunChecks {
		// The main pre-run check is the MySQL version, and we insist on MySQL 8.0
		// This is because we plan to remove 5.7 support in the very near feature,
		// and don't want to claim support for it only to lift the rug out from
		// under users so quickly after release.
		if err := migration.runChecks(context.TODO(), check.ScopePreRun); err != nil {
			return err
		}
	}
	if err := migration.Run(context.TODO()); err != nil {
		return err
	}
	return nil
}

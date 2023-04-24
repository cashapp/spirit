// Package migration contains the logic for running online schema changes.
package migration

import (
	"context"
	"time"
)

type Migration struct {
	Host              string        `name:"host" help:"Hostname" optional:"" default:"127.0.0.1:3306"`
	Username          string        `name:"username" help:"User" optional:"" default:"msandbox"`
	Password          string        `name:"password" help:"Password" optional:"" default:"msandbox"`
	Database          string        `name:"database" help:"Database" optional:"" default:"test"`
	Table             string        `name:"table" help:"Table" optional:"" default:"stock"`
	Alter             string        `name:"alter" help:"The alter statement to run on the table" optional:"" default:"engine=innodb"`
	Threads           int           `name:"threads" help:"Number of concurrent threads for copy and checksum tasks" optional:"" default:"4"`
	TargetChunkTime   time.Duration `name:"target-chunk-time" help:"The target copy time for each chunk" optional:"" default:"500ms"`
	AttemptInplaceDDL bool          `name:"attempt-inplace-ddl" help:"Attempt inplace DDL (only safe without replicas or with Aurora Global)" optional:"" default:"false"`
	Checksum          bool          `name:"checksum" help:"Checksum new table before final cut-over" optional:"" default:"true"`
	ReplicaDSN        string        `name:"replica-dsn" help:"A DSN for a replica which (if specified) will be used for lag checking." optional:""`
	ReplicaMaxLag     time.Duration `name:"replica-max-lag" help:"The maximum lag allowed on the replica before the migration throttles." optional:"" default:"120s"`
}

func (m *Migration) Run() error {
	migration, err := NewRunner(m)
	if err != nil {
		return err
	}
	defer migration.Close()
	if err := migration.Run(context.TODO()); err != nil {
		return err
	}
	return nil
}

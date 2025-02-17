// Package migration contains the logic for running online schema changes.
package migration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cashapp/spirit/pkg/check"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/cashapp/spirit/pkg/utils"
)

var (
	ErrMismatchedAlter = errors.New("alter statement in checkpoint table does not match the alter statement specified here")
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
	ForceInplace         bool          `name:"force-inplace" help:"Force attempt to use inplace (only safe without replicas or with Aurora Global)" optional:"" default:"false"`
	Checksum             bool          `name:"checksum" help:"Checksum new table before final cut-over" optional:"" default:"true"`
	ReplicaDSN           string        `name:"replica-dsn" help:"A DSN for a replica which (if specified) will be used for lag checking." optional:""`
	ReplicaMaxLag        time.Duration `name:"replica-max-lag" help:"The maximum lag allowed on the replica before the migration throttles." optional:"" default:"120s"`
	LockWaitTimeout      time.Duration `name:"lock-wait-timeout" help:"The DDL lock_wait_timeout required for checksum and cutover" optional:"" default:"30s"`
	SkipDropAfterCutover bool          `name:"skip-drop-after-cutover" help:"Keep old table after completing cutover" optional:"" default:"false"`
	DeferCutOver         bool          `name:"defer-cutover" help:"Defer cutover (and checksum) until sentinel table is dropped" optional:"" default:"false"`
	Strict               bool          `name:"strict" help:"Exit on --alter mismatch when incomplete migration is detected" optional:"" default:"false"`
	InterpolateParams    bool          `name:"interpolate-params" help:"Enable interpolate params for DSN" optional:"" default:"false" hidden:""`
	Statement            string        `name:"statement" help:"The SQL statement to run (replaces --table and --alter)" optional:"" default:""`
}

func (m *Migration) Run() error {
	migration, err := NewRunner(m)
	if err != nil {
		return err
	}
	defer migration.Close()
	if err := migration.runChecks(context.TODO(), check.ScopePreRun); err != nil {
		return err
	}
	if err := migration.Run(context.TODO()); err != nil {
		return err
	}
	return nil
}

// normalizeOptions does some validation and sets defaults.
// for example, it validates that only --statement or --table and --alter are specified.
func (m *Migration) normalizeOptions() error {
	if m.TargetChunkTime == 0 {
		m.TargetChunkTime = table.ChunkerDefaultTarget
	}
	if m.Threads == 0 {
		m.Threads = 4
	}
	if m.ReplicaMaxLag == 0 {
		m.ReplicaMaxLag = 120 * time.Second
	}
	if m.Host == "" {
		return errors.New("host is required")
	}
	if !strings.Contains(m.Host, ":") {
		m.Host = fmt.Sprintf("%s:%d", m.Host, 3306)
	}
	if m.Database == "" {
		return errors.New("schema name is required")
	}
	if m.Statement != "" { // statement is specified
		if m.Table != "" || m.Alter != "" {
			return errors.New("only --statement or --table and --alter can be specified")
		}
		// extract the table and alter from the statement.
		// if it is a CREATE INDEX statement, we rewrite it to an alter statement.
		// This also returns the StmtNode, which we do nothing with so far.
		// TODO: if it's a CREATE INDEX statement, do we store the rewritten
		// form in m.Statement?
		var err error
		m.Table, m.Alter, err = utils.ExtractFromStatement(m.Statement)
		if err != nil {
			return errors.New("could not parse statement")
		}
	} else {
		if m.Table == "" {
			return errors.New("table name is required")
		}
		if m.Alter == "" {
			return errors.New("alter statement is required")
		}
		// Add the statement so that it can be used in some contexts
		// Where the statement is preferred over the table and alter.
		m.Statement = fmt.Sprintf("ALTER TABLE `%s`.`%s` %s", m.Database, m.Table, m.Alter)
	}
	return nil
}

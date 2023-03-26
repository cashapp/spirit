// Package check provides various configuration and health checks
// that can be run against a sql.DB connection.
package check

import (
	"context"
	"database/sql"

	"github.com/siddontang/loggers"
)

// ScopeFlag a checker's scope
type ScopeFlag uint8

const (
	ScopeNone        ScopeFlag = 0
	ScopePreflight   ScopeFlag = 1 << 0
	ScopeCutover     ScopeFlag = 1 << 1
	ScopePostCutover ScopeFlag = 1 << 2
	// These are scopes for replica checks
	// Its a different db which is handed hence the different scope.
	//ScopeReplicaPreflight   ScopeFlag = 1 << 3
	//ScopeReplicaCutover     ScopeFlag = 1 << 4
	//ScopeReplicaPostCutover ScopeFlag = 1 << 5
)

type check struct {
	name     string
	callback func(context.Context, *sql.DB, loggers.Advanced) error
	scope    ScopeFlag
}

var checks []check

// registerCheck registers a check (callback func) and a scope (aka time) that it is expected to be run
func registerCheck(name string, callback func(context.Context, *sql.DB, loggers.Advanced) error, scope ScopeFlag) {
	checks = append(checks, check{name: name, callback: callback, scope: scope})
}

// RunChecks runs all checks that are registered for the given scope
func RunChecks(ctx context.Context, db *sql.DB, logger loggers.Advanced, scope ScopeFlag) error {
	for _, check := range checks {
		if check.scope&scope == 0 {
			continue
		}
		err := check.callback(ctx, db, logger)
		if err != nil {
			return err
		}
	}
	return nil
}

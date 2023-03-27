// Package check provides various configuration and health checks
// that can be run against a sql.DB connection.
package check

import (
	"context"
	"database/sql"
	"sync"

	"github.com/siddontang/loggers"
	"github.com/squareup/spirit/pkg/table"
)

// ScopeFlag scopes a check
type ScopeFlag uint8

const (
	ScopeNone        ScopeFlag = 0
	ScopePreflight   ScopeFlag = 1 << 0
	ScopePostSetup   ScopeFlag = 1 << 1
	ScopeCutover     ScopeFlag = 1 << 2
	ScopePostCutover ScopeFlag = 1 << 3
)

type Resources struct {
	DB      *sql.DB
	Replica *sql.DB
	Table   *table.TableInfo
	Alter   string
}

type check struct {
	callback func(context.Context, Resources, loggers.Advanced) error
	scope    ScopeFlag
}

var (
	checks map[string]check
	lock   sync.Mutex
)

// registerCheck registers a check (callback func) and a scope (aka time) that it is expected to be run
func registerCheck(name string, callback func(context.Context, Resources, loggers.Advanced) error, scope ScopeFlag) {
	lock.Lock()
	defer lock.Unlock()
	if checks == nil {
		checks = make(map[string]check)
	}
	checks[name] = check{callback: callback, scope: scope}
}

// RunChecks runs all checks that are registered for the given scope
func RunChecks(ctx context.Context, r Resources, logger loggers.Advanced, scope ScopeFlag) error {
	for _, check := range checks {
		if check.scope&scope == 0 {
			continue
		}
		err := check.callback(ctx, r, logger)
		if err != nil {
			return err
		}
	}
	return nil
}

package check

import (
	"context"

	"github.com/siddontang/loggers"
)

func init() {
	registerCheck("illegalClause", illegalClauseCheck, ScopePreflight)
}

// illegalClauseCheck checks for the presence of specific, unsupported
// clauses in the ALTER statement, such as ALGORITHM= and LOCK=.
func illegalClauseCheck(ctx context.Context, r Resources, logger loggers.Advanced) error {
	return r.Statement.AlterContainsUnsupportedClause()
}

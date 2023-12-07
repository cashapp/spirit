package check

import (
	"context"
	"fmt"

	"github.com/cashapp/spirit/pkg/utils"
	"github.com/siddontang/loggers"
)

func init() {
	registerCheck("illegalClause", illegalClauseCheck, ScopePreRun)
}

// renameCheck checks for any renames, which are not supported.
func illegalClauseCheck(ctx context.Context, r Resources, logger loggers.Advanced) error {
	sql := fmt.Sprintf("ALTER TABLE x.x %s", r.Alter)
	return utils.AlterContainsUnsupportedClause(sql)
}

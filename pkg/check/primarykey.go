package check

import (
	"context"
	"errors"

	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/siddontang/loggers"
)

func init() {
	registerCheck("primarykey", primaryKeyCheck, ScopePreflight)
}

func primaryKeyCheck(ctx context.Context, r Resources, logger loggers.Advanced) error {
	alterStmt, ok := (*r.Statement.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return errors.New("not a valid alter table statement")
	}
	for _, spec := range alterStmt.Specs {
		if spec.Tp == ast.AlterTableDropPrimaryKey {
			return errors.New("dropping primary key is not supported")
		}
	}
	return nil // no problems
}

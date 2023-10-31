package check

import (
	"context"
	"errors"
	"fmt"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/siddontang/loggers"
)

func init() {
	registerCheck("primarykey", primaryKeyCheck, ScopePreflight)
}

func primaryKeyCheck(ctx context.Context, r Resources, logger loggers.Advanced) error {
	sql := fmt.Sprintf("ALTER TABLE %s %s", r.Table.TableName, r.Alter)
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return fmt.Errorf("could not parse alter table statement: %s", sql)
	}
	stmt := &stmtNodes[0]
	alterStmt, ok := (*stmt).(*ast.AlterTableStmt)
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

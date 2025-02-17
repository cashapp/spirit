package check

import (
	"context"
	"errors"
	"fmt"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/siddontang/loggers"
)

func init() {
	registerCheck("dropadd", dropAddCheck, ScopePreflight)
}

// dropAddCheck checks for a DROP and then ADD in the same statement.
// This is unsupported per https://github.com/cashapp/spirit/issues/102
// The actual implementation is a bit simpler:
//   - We only allow a column name to be mentioned once across all
//     DROP and ADD parts of the alter statement.
func dropAddCheck(ctx context.Context, r Resources, logger loggers.Advanced) error {
	p := parser.New()
	stmtNodes, _, err := p.Parse(r.Statement, "", "")
	if err != nil {
		return fmt.Errorf("could not parse alter table statement: %s", r.Statement)
	}
	stmt := &stmtNodes[0]
	alterStmt, ok := (*stmt).(*ast.AlterTableStmt)
	if !ok {
		return errors.New("not a valid alter table statement")
	}
	columnsUsed := make(map[string]int)
	for _, spec := range alterStmt.Specs {
		if spec.Tp == ast.AlterTableDropColumn {
			columnsUsed[spec.OldColumnName.String()]++
		}
		if spec.Tp == ast.AlterTableAddColumns {
			for _, col := range spec.NewColumns {
				columnsUsed[col.Name.String()]++
			}
		}
	}
	for col, count := range columnsUsed {
		if count > 1 {
			return fmt.Errorf("column %s is mentioned %d times in the same statement", col, count)
		}
	}
	return nil // safe
}

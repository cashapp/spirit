package check

import (
	"context"
	"errors"
	"fmt"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/siddontang/loggers"
)

func init() {
	registerCheck("rename", renameCheck, ScopePreflight)
}

// renameCheck checks for any renames, which are not supported.
func renameCheck(ctx context.Context, r Resources, logger loggers.Advanced) error {
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
		if spec.Tp == ast.AlterTableRenameTable || spec.Tp == ast.AlterTableRenameColumn {
			return errors.New("renames are not supported")
		}
		// ALTER TABLE CHANGE COLUMN can be used to rename a column.
		// But they can also be used commonly without a rename, so the check needs to be deeper.
		if spec.Tp == ast.AlterTableChangeColumn {
			if spec.NewColumns[0].Name.String() != spec.OldColumnName.String() {
				return errors.New("renames are not supported")
			}
		}
	}
	return nil // no renames
}

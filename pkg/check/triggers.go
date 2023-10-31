package check

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/siddontang/loggers"
)

func init() {
	registerCheck("addtriggers", addTriggersCheck, ScopePreflight)
	registerCheck("hastriggers", hasTriggersCheck, ScopePreflight)
}

// hasTriggersCheck check if table has triggers associated with it, which is not supported
func hasTriggersCheck(ctx context.Context, r Resources, logger loggers.Advanced) error {
	sql := `SELECT * FROM information_schema.triggers WHERE 
	(event_object_schema=? AND event_object_table=?)`
	rows, err := r.DB.QueryContext(ctx, sql, r.Table.SchemaName, r.Table.TableName)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return errors.New("tables with triggers associated are not supported")
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}

// addTriggersCheck checks for trigger creations, which is not supported
func addTriggersCheck(ctx context.Context, r Resources, logger loggers.Advanced) error {
	isAddingTrigger := strings.Contains(strings.ToUpper(r.Alter), "CREATE TRIGGER")
	if isAddingTrigger {
		return errors.New("adding triggers is not supported")
	}
	sql := fmt.Sprintf("ALTER TABLE %s %s", r.Table.TableName, r.Alter)
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return fmt.Errorf("could not parse alter table statement: %s", sql)
	}
	stmt := &stmtNodes[0]
	_, ok := (*stmt).(*ast.AlterTableStmt)
	if !ok {
		return errors.New("not a valid alter table statement")
	}
	return nil // no problems
}

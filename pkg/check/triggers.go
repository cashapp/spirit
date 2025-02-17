package check

import (
	"context"
	"errors"
	"strings"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
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
	if r.DB == nil {
		return errors.New("no database connection")
	}
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
// Since the TiDB parser doesn't support this, we are using strings.Contains :(
func addTriggersCheck(ctx context.Context, r Resources, logger loggers.Advanced) error {
	isAddingTrigger := strings.Contains(strings.ToUpper(strings.TrimSpace(r.Statement)), "CREATE TRIGGER")
	if isAddingTrigger {
		return errors.New("adding triggers is not supported")
	}
	return nil // no problems
}

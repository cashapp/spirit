// Package utils contains some common utilities used by all other packages.
package utils

import (
	"errors"
	"fmt"
	"strings"

	"github.com/cashapp/spirit/pkg/dbconn/sqlescape"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
)

const (
	PrimaryKeySeparator = "-#-" // used to hash a composite primary key
)

// HashKey is used to convert a composite key into a string
// so that it can be placed in a map.
func HashKey(key []interface{}) string {
	var pk []string
	for _, v := range key {
		pk = append(pk, fmt.Sprintf("%v", v))
	}
	return strings.Join(pk, PrimaryKeySeparator)
}

// IntersectNonGeneratedColumns returns a string of columns that are in both tables
func IntersectNonGeneratedColumns(t1, t2 *table.TableInfo) string {
	var intersection []string
	for _, col := range t1.NonGeneratedColumns {
		for _, col2 := range t2.NonGeneratedColumns {
			if col == col2 {
				intersection = append(intersection, "`"+col+"`")
			}
		}
	}
	return strings.Join(intersection, ", ")
}

// UnhashKey converts a hashed key to a string that can be used in a query.
func UnhashKey(key string) string {
	str := strings.Split(key, PrimaryKeySeparator)
	if len(str) == 1 {
		return "'" + sqlescape.EscapeString(str[0]) + "'"
	}
	for i, v := range str {
		str[i] = "'" + sqlescape.EscapeString(v) + "'"
	}
	return "(" + strings.Join(str, ",") + ")"
}

// ErrInErr is a wrapper func to not nest too deeply in an error being handled
// inside of an already error path. Not catching the error makes linters unhappy,
// but because it's already in an error path, there's not much to do.
func ErrInErr(_ error) {
}

func StripPort(hostname string) string {
	if strings.Contains(hostname, ":") {
		return strings.Split(hostname, ":")[0]
	}
	return hostname
}

// AlgorithmInplaceConsideredSafe checks to see if all clauses of an ALTER
// statement are "safe". We consider an operation to be "safe" if it is "In
// Place" and "Only Modifies Metadata". See
// https://dev.mysql.com/doc/refman/8.0/en/innodb-online-ddl-operations.html
// for details.
// INPLACE DDL is not generally safe for online use in MySQL 8.0, because ADD
// INDEX can block replicas.
func AlgorithmInplaceConsideredSafe(sql string) error {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return err
	}
	stmt := &stmtNodes[0]
	alterStmt, ok := (*stmt).(*ast.AlterTableStmt)
	if !ok {
		return err
	}

	// There can be multiple clauses in a single ALTER TABLE statement.
	// If all of them are safe, we can attempt to use INPLACE.
	unsafeClauses := 0
	for _, spec := range alterStmt.Specs {
		switch spec.Tp {
		case ast.AlterTableDropIndex,
			ast.AlterTableRenameIndex,
			ast.AlterTableIndexInvisible:
			continue
		default:
			unsafeClauses++
		}
	}
	if unsafeClauses > 0 {
		if len(alterStmt.Specs) > 1 {
			return errors.New("ALTER contains multiple clauses. Combinations of INSTANT and INPLACE operations cannot be detected safely. Consider executing these as separate ALTER statements. Use --force-inplace to override this safety check")
		}
		return errors.New("ALTER either does not support INPLACE or when performed as INPLACE could take considerable time. Use --force-inplace to override this safety check")
	}
	return nil
}

// AlterContainsUnsupportedClause checks to see if any clauses of an ALTER
// statement are unsupported by Spirit. These include clauses like ALGORITHM
// and LOCK, because they step on the toes of Spirit's own locking and
// algorithm selection.
func AlterContainsUnsupportedClause(sql string) error {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return err
	}
	stmt := &stmtNodes[0]
	alterStmt, ok := (*stmt).(*ast.AlterTableStmt)
	if !ok {
		return err
	}

	var unsupportedClauses []string
	for _, spec := range alterStmt.Specs {
		switch spec.Tp {
		case ast.AlterTableAlgorithm:
			unsupportedClauses = append(unsupportedClauses, "ALGORITHM=")
		case ast.AlterTableLock:
			unsupportedClauses = append(unsupportedClauses, "LOCK=")
		default:
		}
	}
	if len(unsupportedClauses) > 0 {
		unsupportedClause := strings.Join(unsupportedClauses, ", ")
		return fmt.Errorf("ALTER contains unsupported clause(s): %s", unsupportedClause)
	}
	return nil
}

// AlterContainsAddUnique checks to see if any clauses of an ALTER contains add UNIQUE index.
// We use this to customize the error returned from checksum fails.
func AlterContainsAddUnique(sql string) error {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return err
	}
	stmt := &stmtNodes[0]
	alterStmt, ok := (*stmt).(*ast.AlterTableStmt)
	if !ok {
		return err
	}
	for _, spec := range alterStmt.Specs {
		if spec.Tp == ast.AlterTableAddConstraint && spec.Constraint.Tp == ast.ConstraintUniq {
			return errors.New("contains adding a unique index")
		}
	}
	return nil
}

// AlterContainsIndexVisibility checks to see if there are any clauses of an ALTER to change index visibility.
// It really does not make sense for visibility changes to be anything except metadata only changes,
// because they are used for experiments. An experiment is not rebuilding the table. If you are experimenting
// setting an index to invisible and plan to switch it back to visible quickly if required, going through
// a full table rebuild does not make sense.
func AlterContainsIndexVisibility(sql string) error {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return err
	}
	stmt := &stmtNodes[0]
	alterStmt, ok := (*stmt).(*ast.AlterTableStmt)
	if !ok {
		return err
	}
	for _, spec := range alterStmt.Specs {
		if spec.Tp == ast.AlterTableIndexInvisible {
			return errors.New("the ALTER operation contains a change to index visibility and could not be completed as a meta-data only operation. This is a safety check! Please split the ALTER statement into separate statements for changing the invisible index and other operations")
		}
	}
	return nil
}

func TrimAlter(alter string) string {
	return strings.TrimSuffix(strings.TrimSpace(alter), ";")
}

func ExtractFromStatement(statement string) (table string, alter string, err error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(statement, "", "")
	if err != nil {
		return "", "", err
	}
	if len(stmtNodes) != 1 {
		return "", "", errors.New("only one statement may be specified at once")
	}
	switch stmtNodes[0].(type) {
	case *ast.AlterTableStmt:
		// type assert stmtNodes[0] as an AlterTableStmt and then
		// extract the table and alter from it.
		// TODO: handle the database name correctly, as it might differ from
		// what was specified as --database.
		alterStmt := stmtNodes[0].(*ast.AlterTableStmt)
		var sb strings.Builder
		sb.Reset()
		rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
		if err = alterStmt.Restore(rCtx); err != nil {
			return "", "", fmt.Errorf("could not restore alter table statement: %s", err)
		}
		normalizedStmt := sb.String()
		trimLen := len(alterStmt.Table.Name.String()) + 15 // len ALTER TABLE + quotes
		if len(alterStmt.Table.Schema.String()) > 0 {
			trimLen += len(alterStmt.Table.Schema.String()) + 3 // len schema + quotes and dot.
		}
		return alterStmt.Table.Name.String(), normalizedStmt[trimLen:], nil
	case *ast.CreateIndexStmt:
		// Need to rewrite to a corresponding ALTER TABLE statement
		table, alter, err = convertCreateIndexToAlterTable(stmtNodes[0])
		return table, alter, err
	// returning an empty alter means we are allowed to run it
	// but it's not a spirit migration. But the table should be specified.
	case *ast.CreateTableStmt:
		createStmt := stmtNodes[0].(*ast.CreateTableStmt)
		return createStmt.Table.Name.String(), "", nil
	case *ast.DropTableStmt:
		dropStmt := stmtNodes[0].(*ast.DropTableStmt)
		return dropStmt.Tables[0].Name.String(), "", nil
	case *ast.RenameTableStmt:
		renameStmt := stmtNodes[0].(*ast.RenameTableStmt)
		return renameStmt.TableToTables[0].OldTable.Name.String(), "", nil
	}
	// default:
	return "", "", errors.New("not a supported statement type")
}

func convertCreateIndexToAlterTable(stmt ast.StmtNode) (table string, alter string, err error) {
	ciStmt, isCreateIndexStmt := stmt.(*ast.CreateIndexStmt)
	if !isCreateIndexStmt {
		return "", "", errors.New("not a CREATE INDEX statement")
	}
	var columns []string
	var keyType string
	for _, part := range ciStmt.IndexPartSpecifications {
		columns = append(columns, part.Column.Name.String())
	}
	switch ciStmt.KeyType {
	case ast.IndexKeyTypeUnique:
		keyType = "UNIQUE INDEX"
	case ast.IndexKeyTypeFullText:
		keyType = "FULLTEXT INDEX"
	case ast.IndexKeyTypeSpatial:
		keyType = "SPATIAL INDEX"
	default:
		keyType = "INDEX"
	}
	return ciStmt.Table.Name.String(), fmt.Sprintf("ADD %s %s (%s)", keyType, ciStmt.IndexName, strings.Join(columns, ", ")), nil
}

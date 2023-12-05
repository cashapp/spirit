// Package utils contains some common utilities used by all other packages.
package utils

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/cashapp/spirit/pkg/table"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
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

// IntersectColumns returns a string of columns that are in both tables.
func IntersectColumns(t1, t2 *table.TableInfo) string {
	var intersection []string
	for _, col := range t1.Columns {
		for _, col2 := range t2.Columns {
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

// IsMySQL8 returns true if we can positively identify this as mysql 8
func IsMySQL8(db *sql.DB) bool {
	var version string
	if err := db.QueryRow("select substr(version(), 1, 1)").Scan(&version); err != nil {
		return false // can't tell
	}
	return version == "8"
}

func StripPort(hostname string) string {
	if strings.Contains(hostname, ":") {
		return strings.Split(hostname, ":")[0]
	}
	return hostname
}

func AlgorithmInplaceConsideredSafe(sql string) error {
	// INPLACE DDL is not generally safe for online use in MySQL 8.0,
	// because ADD INDEX can block replicas. Some INPLACE operations
	// *are* safe, because they only modify table metadata,
	// so we parse the ALTER and attempt to use the INPLACE
	// algorithm for statements that in practice execute instantly.

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
			ast.AlterTableRenameIndex:
			continue
		default:
			unsafeClauses++
		}
	}
	if unsafeClauses > 0 {
		if len(alterStmt.Specs) > 1 {
			return fmt.Errorf("some clause(s) of the ALTER staement are not INPLACE safe (note: combinations of INSTANT and INPLACE operations can *not* currently be detected to be safe; consider executing these as separate ALTER statements)")
		}
		return fmt.Errorf("ALTER TABLE statement is not safe for INPLACE")
	}
	return nil
}

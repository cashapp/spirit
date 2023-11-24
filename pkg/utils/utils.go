// Package utils contains some common utilities used by all other packages.
package utils

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/cashapp/spirit/pkg/table"
	"github.com/hashicorp/go-version"
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

func IsMariaDB(db *sql.DB) bool {
	var versionString string
	if err := db.QueryRow("select version()").Scan(&versionString); err != nil {
		return false // can't tell
	}
	return strings.Contains(versionString, "MariaDB")
}

// IsMySQL8 returns true if we can positively identify this as mysql 8
func IsMySQL8(db *sql.DB) bool {
	var versionString string
	if err := db.QueryRow("select version()").Scan(&versionString); err != nil {
		return false // can't tell
	}

	// remove any part of the string that could be interpreted as a prerelease
	parsed, err := version.NewVersion(strings.Split(versionString, "-")[0])
	if err != nil {
		return false // can't tell
	}

	// decide on constraint. examples: 8.0.27, 10.5.23-MariaDB-log
	// 8.0.13 allows rename tables while locked
	constraint := ">= 8.0.13"
	if strings.Contains(versionString, "MariaDB") {
		// MariaDB 10.4 and up are based on MySQL 8.0
		constraint = ">= 10.4"
	}

	constraints, err := version.NewConstraint(constraint)
	if err != nil {
		return false // can't tell
	}

	return constraints.Check(parsed)
}

func StripPort(hostname string) string {
	if strings.Contains(hostname, ":") {
		return strings.Split(hostname, ":")[0]
	}
	return hostname
}

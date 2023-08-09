// Package utils contains some common utilities used by all other packages.
package utils

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/squareup/spirit/pkg/table"
)

const (
	PrimaryKeySeparator = "-#-" // used to hash a composite primary key
)

// MysqlRealEscapeString escapes a string for use in a query.
// usually the string is a primary key, so the likelihood of a quote is low.
func MysqlRealEscapeString(value string) string {
	var sb strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch c {
		case '\\', 0, '\n', '\r', '\'', '"':
			sb.WriteByte('\\')
			sb.WriteByte(c)
		default:
			sb.WriteByte(c)
		}
	}
	return sb.String()
}

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
		return "'" + MysqlRealEscapeString(str[0]) + "'"
	}
	for i, v := range str {
		str[i] = "'" + MysqlRealEscapeString(v) + "'"
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

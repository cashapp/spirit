package table

import (
	"regexp"
	"sort"
	"strings"
	"time"
)

// lazyFindP90 finds the second to last value in a slice.
// This is the same as a p90 if there are 10 values, but if
// there were 100 values it would technically be a p99 etc.
func lazyFindP90(a []time.Duration) time.Duration {
	sort.Slice(a, func(i, j int) bool {
		return a[i] > a[j]
	})
	return a[len(a)/10]
}

// castableTp returns an approximate type that tp can be casted to.
// This is because in the context of CAST()/CONVERT() MySQL will
// not allow all of the built-in types, but instead follows SQL standard
// types, which we need to map to.
func castableTp(tp string) string {
	newTp := removeWidth(tp)
	newTp = removeEnumSetOpts(newTp)
	newTp = removeZerofill(newTp)
	newTp = removeDecimalWidth(newTp)
	switch newTp {
	case "tinyint", "smallint", "mediumint", "int", "bigint":
		return "signed"
	case "tinyint unsigned", "smallint unsigned", "mediumint unsigned", "int unsigned", "bigint unsigned":
		return "unsigned"
	case "timestamp", "datetime":
		return "datetime"
	case "varchar", "enum", "set", "text", "mediumtext", "longtext":
		return "char"
	case "tinyblob", "blob", "mediumblob", "longblob", "varbinary", "binary":
		return "binary"
	case "float", "double": // required for MySQL 5.7
		return "char"
	case "json":
		return "json"
	case "decimal":
		return tp
	default:
		return "char" // default to char because it ends up being a char string in the concat anyway.
	}
}

func removeWidth(s string) string {
	regex := regexp.MustCompile(`\([0-9]+\)`)
	s = regex.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

func removeDecimalWidth(s string) string {
	regex := regexp.MustCompile(`\([0-9]+,[0-9]+\)`)
	s = regex.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

func removeEnumSetOpts(s string) string {
	if len(s) > 4 && strings.EqualFold(s[:4], "enum") {
		return "enum"
	}
	if len(s) > 3 && strings.EqualFold(s[:3], "set") {
		return "set"
	}
	return s
}

func removeZerofill(s string) string {
	return strings.Replace(s, " zerofill", "", -1)
}

func QuoteColumns(cols []string) string {
	q := make([]string, len(cols))
	for i, col := range cols {
		q[i] = "`" + col + "`"
	}
	return strings.Join(q, ", ")
}

// mysqlRealEscapeString escapes a string for use in a query.
// usually the string is a primary key, so the likelihood of a quote is low.
func mysqlRealEscapeString(value string) string {
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

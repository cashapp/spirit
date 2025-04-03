package repl

import (
	"strings"

	"github.com/cashapp/spirit/pkg/utils"
)

// pksToRowValueConstructor constructs a statement like this:
// DELETE FROM x WHERE (s_i_id,s_w_id) in ((7,10),(1,5));
func pksToRowValueConstructor(d []string) string {
	var pkValues []string
	for _, v := range d {
		pkValues = append(pkValues, utils.UnhashKey(v))
	}
	return strings.Join(pkValues, ",")
}

type statement struct {
	numKeys int
	stmt    string
}

func extractStmt(stmts []statement) []string {
	var trimmed []string
	for _, stmt := range stmts {
		if stmt.stmt != "" {
			trimmed = append(trimmed, stmt.stmt)
		}
	}
	return trimmed
}

func encodeSchemaTable(schema, table string) string {
	return schema + "." + table
}

func decodeSchemaTable(schemaTable string) (string, string) {
	parts := strings.Split(schemaTable, ".")
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

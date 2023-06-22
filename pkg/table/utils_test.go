package table

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFindP90(t *testing.T) {
	times := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		1 * time.Second,
		3 * time.Second,
		10 * time.Second,
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
	}
	assert.Equal(t, 3*time.Second, lazyFindP90(times))
}

type castableTpTest struct {
	tp       string
	expected string
}

func TestCastableTp(t *testing.T) {
	tps := []castableTpTest{
		{"tinyint", "signed"},
		{"smallint", "signed"},
		{"mediumint", "signed"},
		{"int", "signed"},
		{"bigint", "signed"},
		{"tinyint unsigned", "unsigned"},
		{"smallint unsigned", "unsigned"},
		{"mediumint unsigned", "unsigned"},
		{"int unsigned", "unsigned"},
		{"bigint unsigned", "unsigned"},
		{"timestamp", "datetime"},
		{"timestamp(6)", "datetime"},
		{"varchar(100)", "char"},
		{"text", "char"},
		{"mediumtext", "char"},
		{"longtext", "char"},
		{"tinyblob", "binary"},
		{"blob", "binary"},
		{"mediumblob", "binary"},
		{"longblob", "binary"},
		{"varbinary", "binary"},
		{"char(100)", "char"},
		{"binary(100)", "binary"},
		{"datetime", "datetime"},
		{"datetime(6)", "datetime"},
		{"year", "char"},
		{"float", "char"},
		{"double", "char"},
		{"json", "json"},
		{"int(11)", "signed"},
		{"int(11) unsigned", "unsigned"},
		{"int(11) zerofill", "signed"},
		{"int(11) unsigned zerofill", "unsigned"},
		{"enum('a', 'b', 'c')", "char"},
		{"set('a', 'b', 'c')", "char"},
		{"decimal(6,2)", "decimal(6,2)"},
	}
	for _, tp := range tps {
		assert.Equal(t, tp.expected, castableTp(tp.tp))
	}
}

func TestQuoteCols(t *testing.T) {
	cols := []string{"a", "b", "c"}
	assert.Equal(t, "`a`, `b`, `c`", QuoteColumns(cols))

	cols = []string{"a"}
	assert.Equal(t, "`a`", QuoteColumns(cols))
}

func TestMySQLRealEscapeString(t *testing.T) {
	assert.Equal(t, "abc", mysqlRealEscapeString("abc"))
	assert.Equal(t, `o\'test`, mysqlRealEscapeString(`o'test`))
	assert.Equal(t, `o\\\'test`, mysqlRealEscapeString(`o\'test`))
}

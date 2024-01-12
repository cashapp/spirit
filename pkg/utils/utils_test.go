package utils

import (
	"testing"

	"github.com/cashapp/spirit/pkg/table"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/assert"
)

func TestIntersectColumns(t *testing.T) {
	t1 := table.NewTableInfo(nil, "test", "t1")
	t1new := table.NewTableInfo(nil, "test", "t1_new")
	t1.Columns = []string{"a", "b", "c"}
	t1new.Columns = []string{"a", "b", "c"}
	str := IntersectColumns(t1, t1new)
	assert.Equal(t, "`a`, `b`, `c`", str)

	t1new.Columns = []string{"a", "c"}
	str = IntersectColumns(t1, t1new)
	assert.Equal(t, "`a`, `c`", str)

	t1new.Columns = []string{"a", "c", "d"}
	str = IntersectColumns(t1, t1new)
	assert.Equal(t, "`a`, `c`", str)
}

func TestHashAndUnhashKey(t *testing.T) {
	// This func helps put composite keys in a map.
	key := []interface{}{"1234", "ACDC", "12"}
	hashed := HashKey(key)
	assert.Equal(t, "1234-#-ACDC-#-12", hashed)
	unhashed := UnhashKey(hashed)
	// unhashed returns as a string, not the original interface{}
	assert.Equal(t, "('1234','ACDC','12')", unhashed)

	// This also works on single keys.
	key = []interface{}{"1234"}
	hashed = HashKey(key)
	assert.Equal(t, "1234", hashed)
	unhashed = UnhashKey(hashed)
	assert.Equal(t, "'1234'", unhashed)
}

func TestStripPort(t *testing.T) {
	assert.Equal(t, "hostname.com", StripPort("hostname.com"))
	assert.Equal(t, "hostname.com", StripPort("hostname.com:3306"))
	assert.Equal(t, "127.0.0.1", StripPort("127.0.0.1:3306"))
}

func TestAlgorithmInplaceConsideredSafe(t *testing.T) {
	var alter = func(stmt string) string {
		return "ALTER TABLE `test`.`t1` " + stmt
	}
	assert.NoError(t, AlgorithmInplaceConsideredSafe(alter("drop index `a`")))
	assert.NoError(t, AlgorithmInplaceConsideredSafe(alter("rename index `a` to `b`")))
	assert.NoError(t, AlgorithmInplaceConsideredSafe(alter("drop index `a`, drop index `b`")))
	assert.NoError(t, AlgorithmInplaceConsideredSafe(alter("drop index `a`, rename index `b` to c")))

	assert.Error(t, AlgorithmInplaceConsideredSafe(alter("ADD COLUMN `a` INT")))
	assert.Error(t, AlgorithmInplaceConsideredSafe(alter("ADD index (a)")))
	assert.Error(t, AlgorithmInplaceConsideredSafe(alter("drop index `a`, add index `b` (`b`)")))
	assert.Error(t, AlgorithmInplaceConsideredSafe(alter("engine=innodb")))
	// this *should* be safe, but we don't support it yet because we can't
	// guess which operations are INSTANT
	assert.Error(t, AlgorithmInplaceConsideredSafe(alter("drop index `a`, add column `b` int")))
}

func TestAlterIsAddUnique(t *testing.T) {
	var alter = func(stmt string) string {
		return "ALTER TABLE `test`.`t1` " + stmt
	}
	assert.NoError(t, AlterContainsAddUnique(alter("drop index `a`")))
	assert.NoError(t, AlterContainsAddUnique(alter("rename index `a` to `b`")))
	assert.NoError(t, AlterContainsAddUnique(alter("drop index `a`, drop index `b`")))
	assert.NoError(t, AlterContainsAddUnique(alter("drop index `a`, rename index `b` to c")))

	assert.NoError(t, AlterContainsAddUnique(alter("ADD COLUMN `a` INT")))
	assert.NoError(t, AlterContainsAddUnique(alter("ADD index (a)")))
	assert.NoError(t, AlterContainsAddUnique(alter("drop index `a`, add index `b` (`b`)")))
	assert.NoError(t, AlterContainsAddUnique(alter("engine=innodb")))
	assert.Error(t, AlterContainsAddUnique(alter("add unique(b)"))) // this is potentially lossy.
}

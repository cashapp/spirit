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
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"a", "b", "c"}
	str := IntersectNonGeneratedColumns(t1, t1new)
	assert.Equal(t, "`a`, `b`, `c`", str)

	t1new.NonGeneratedColumns = []string{"a", "c"}
	str = IntersectNonGeneratedColumns(t1, t1new)
	assert.Equal(t, "`a`, `c`", str)

	t1new.NonGeneratedColumns = []string{"a", "c", "d"}
	str = IntersectNonGeneratedColumns(t1, t1new)
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
	assert.NoError(t, AlgorithmInplaceConsideredSafe(alter("ALTER INDEX b INVISIBLE")))
	assert.NoError(t, AlgorithmInplaceConsideredSafe(alter("ALTER INDEX b VISIBLE")))

	assert.Error(t, AlgorithmInplaceConsideredSafe(alter("ADD COLUMN `a` INT")))
	assert.Error(t, AlgorithmInplaceConsideredSafe(alter("ADD index (a)")))
	assert.Error(t, AlgorithmInplaceConsideredSafe(alter("drop index `a`, add index `b` (`b`)")))
	assert.Error(t, AlgorithmInplaceConsideredSafe(alter("engine=innodb")))
	// this *should* be safe, but we don't support it yet because we can't
	// guess which operations are INSTANT
	assert.Error(t, AlgorithmInplaceConsideredSafe(alter("drop index `a`, add column `b` int")))
	assert.Error(t, AlgorithmInplaceConsideredSafe(alter("ALTER INDEX b INVISIBLE, add column `c` int")))
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

func TestAlterContainsIndexVisibility(t *testing.T) {
	var alter = func(stmt string) string {
		return "ALTER TABLE `test`.`t1` " + stmt
	}
	assert.NoError(t, AlterContainsIndexVisibility(alter("drop index `a`")))
	assert.NoError(t, AlterContainsIndexVisibility(alter("rename index `a` to `b`")))
	assert.NoError(t, AlterContainsIndexVisibility(alter("drop index `a`, drop index `b`")))
	assert.NoError(t, AlterContainsIndexVisibility(alter("drop index `a`, rename index `b` to c")))

	assert.NoError(t, AlterContainsIndexVisibility(alter("ADD COLUMN `a` INT")))
	assert.NoError(t, AlterContainsIndexVisibility(alter("ADD index (a)")))
	assert.NoError(t, AlterContainsIndexVisibility(alter("drop index `a`, add index `b` (`b`)")))
	assert.NoError(t, AlterContainsIndexVisibility(alter("engine=innodb")))
	assert.NoError(t, AlterContainsIndexVisibility(alter("add unique(b)")))

	assert.Error(t, AlterContainsIndexVisibility(alter("ALTER INDEX b INVISIBLE")))
	assert.Error(t, AlterContainsIndexVisibility(alter("ALTER INDEX b VISIBLE")))
}

func TestTrimAlter(t *testing.T) {
	assert.Equal(t, "ADD COLUMN `a` INT", TrimAlter("ADD COLUMN `a` INT"))
	assert.Equal(t, "engine=innodb", TrimAlter("engine=innodb;"))
	assert.Equal(t, "engine=innodb", TrimAlter("engine=innodb; "))
	assert.Equal(t, "add column a, add column b", TrimAlter("add column a, add column b;"))
	assert.Equal(t, "add column a, add column b", TrimAlter("add column a, add column b"))
}

func TestExtractFromStatement(t *testing.T) {
	table, alter, err := ExtractFromStatement("ALTER TABLE t1 ADD INDEX (something)")
	assert.NoError(t, err)
	assert.Equal(t, "t1", table)
	assert.Equal(t, "ADD INDEX(`something`)", alter)

	table, alter, err = ExtractFromStatement("ALTER TABLE t.t1aaaa ADD COLUMN newcol int")
	assert.NoError(t, err)
	assert.Equal(t, "t1aaaa", table)
	assert.Equal(t, "ADD COLUMN `newcol` INT", alter)

	table, alter, err = ExtractFromStatement("ALTER TABLE t.t1 DROP COLUMN foo")
	assert.NoError(t, err)
	assert.Equal(t, "t1", table)
	assert.Equal(t, "DROP COLUMN `foo`", alter)

	table, alter, err = ExtractFromStatement("CREATE TABLE t1 (a int)")
	assert.NoError(t, err)
	assert.Equal(t, "t1", table)
	assert.Empty(t, alter)
}

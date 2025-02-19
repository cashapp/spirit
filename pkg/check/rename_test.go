package check

import (
	"context"
	"testing"

	"github.com/cashapp/spirit/pkg/statement"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestRename(t *testing.T) {
	r := Resources{
		Statement: statement.MustNew("ALTER TABLE t1 RENAME TO newtablename"),
	}
	err := renameCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "renames are not supported")

	r.Statement = statement.MustNew("ALTER TABLE t1 RENAME COLUMN c1 TO c2")
	err = renameCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "renames are not supported")

	r.Statement = statement.MustNew("ALTER TABLE t1 CHANGE c1 c2 VARCHAR(100)")
	err = renameCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "renames are not supported")

	r.Statement = statement.MustNew("ALTER TABLE t1 CHANGE c1 c1 VARCHAR(100)") //nolint: dupword
	err = renameCheck(context.Background(), r, logrus.New())
	assert.NoError(t, err)

	r.Statement = statement.MustNew("ALTER TABLE t1 ADD INDEX (anothercol)")
	err = renameCheck(context.Background(), r, logrus.New())
	assert.NoError(t, err) // safe modification
}

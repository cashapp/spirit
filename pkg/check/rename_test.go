package check

import (
	"context"
	"testing"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"

	"github.com/cashapp/spirit/pkg/table"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestRename(t *testing.T) {
	r := Resources{
		Table: &table.TableInfo{TableName: "test"},
		Alter: "RENAME TO newtablename",
	}
	err := renameCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "renames are not supported")

	r = Resources{
		Table: &table.TableInfo{TableName: "test"},
		Alter: "RENAME COLUMN c1 TO c2",
	}
	err = renameCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "renames are not supported")

	r = Resources{
		Table: &table.TableInfo{TableName: "test"},
		Alter: "CHANGE c1 c2 VARCHAR(100)",
	}
	err = renameCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "renames are not supported")

	r = Resources{
		Table: &table.TableInfo{TableName: "test"},
		Alter: "CHANGE c1 c1 VARCHAR(100)", //nolint: dupword
	}
	err = renameCheck(context.Background(), r, logrus.New())
	assert.NoError(t, err)

	r = Resources{
		Table: &table.TableInfo{TableName: "test"},
		Alter: "ADD INDEX (anothercol)",
	}
	err = renameCheck(context.Background(), r, logrus.New())
	assert.NoError(t, err) // safe modification

	r = Resources{
		Table: &table.TableInfo{TableName: "test"},
		Alter: "gibberish",
	}
	err = renameCheck(context.Background(), r, logrus.New())
	assert.Error(t, err) // gibberish
}

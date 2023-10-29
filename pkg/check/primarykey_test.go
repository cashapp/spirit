package check

import (
	"context"
	"testing"

	"github.com/cashapp/spirit/pkg/table"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestPrimaryKey(t *testing.T) {
	r := Resources{
		Table: &table.TableInfo{TableName: "test"},
		Alter: "DROP PRIMARY KEY, ADD PRIMARY KEY (anothercol)",
	}
	err := primaryKeyCheck(context.Background(), r, logrus.New())
	assert.Error(t, err) // drop primary key

	r = Resources{
		Table: &table.TableInfo{TableName: "test"},
		Alter: "ADD INDEX (anothercol)",
	}
	err = primaryKeyCheck(context.Background(), r, logrus.New())
	assert.NoError(t, err) // safe modification

	r = Resources{
		Table: &table.TableInfo{TableName: "test"},
		Alter: "gibberish",
	}
	err = primaryKeyCheck(context.Background(), r, logrus.New())
	assert.Error(t, err) // gibberish
}

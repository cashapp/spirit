package check

import (
	"context"
	"testing"

	"github.com/cashapp/spirit/pkg/table"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestIllegalClauseCheck(t *testing.T) {
	r := Resources{
		Table: &table.TableInfo{TableName: "test"},
		Alter: "ALGORITHM=INPLACE",
	}
	err := illegalClauseCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "ALTER contains unsupported clause")

	r = Resources{
		Table: &table.TableInfo{TableName: "test"},
		Alter: "ALGORITHM=INPLACE, LOCK=shared",
	}
	err = illegalClauseCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "ALTER contains unsupported clause")

	r = Resources{
		Table: &table.TableInfo{TableName: "test"},
		Alter: "lock=none",
	}
	err = illegalClauseCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "ALTER contains unsupported clause")

	r = Resources{
		Table: &table.TableInfo{TableName: "test"},
		Alter: "engine=innodb, algorithm=copy",
	}
	err = illegalClauseCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "ALTER contains unsupported clause")
}

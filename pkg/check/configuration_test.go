package check

import (
	"context"
	"database/sql"
	"testing"

	"github.com/cashapp/spirit/pkg/table"
	"github.com/cashapp/spirit/pkg/testutils"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestConfiguration(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)

	r := Resources{
		DB:    db,
		Table: &table.TableInfo{TableName: "test", SchemaName: "test"},
	}

	err = configurationCheck(context.Background(), r, logrus.New())
	assert.NoError(t, err) // all looks good of course.

	// Binlog row image is dynamic, so we can change it.
	_, err = db.Exec("SET GLOBAL binlog_row_image = 'MINIMAL'")
	assert.NoError(t, err)

	err = configurationCheck(context.Background(), r, logrus.New())
	assert.Error(t, err)

	_, err = db.Exec("SET GLOBAL binlog_row_image = 'FULL'")
	assert.NoError(t, err)
}

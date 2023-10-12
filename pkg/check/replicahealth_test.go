package check

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/pingcap/tidb/parser/test_driver"

	"github.com/cashapp/spirit/pkg/table"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestReplicaHealth(t *testing.T) {
	r := Resources{
		Table: &table.TableInfo{TableName: "test"},
		Alter: "RENAME TO newtablename",
	}
	err := replicaHealth(context.Background(), r, logrus.New())
	assert.NoError(t, err) // if no replica, it returns no error.

	// use a non-replica. this will return an error.
	r.Replica, err = sql.Open("mysql", dsn())
	assert.NoError(t, err)
	err = replicaHealth(context.Background(), r, logrus.New())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "replica is not healthy")

	// use an actual replica
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping test because REPLICA_DSN not set")
	}
	r.Replica, err = sql.Open("mysql", replicaDSN)
	assert.NoError(t, err)
	err = replicaHealth(context.Background(), r, logrus.New())
	assert.NoError(t, err) // all looks good of course.

	// use a completely invalid DSN.
	// golang sql.Open lazy loads, so this is possible.
	r.Replica, err = sql.Open("mysql", "msandbox:msandbox@tcp(127.0.0.1:22)/test")
	assert.NoError(t, err)
	err = replicaHealth(context.Background(), r, logrus.New())
	assert.Error(t, err) // invalid
}

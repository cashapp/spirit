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

func TestReplicaPrivileges(t *testing.T) {
	// use an actual replica
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping test because REPLICA_DSN not set")
	}
	r := Resources{
		Table: &table.TableInfo{TableName: "test"},
		Alter: "RENAME TO newtablename",
	}
	err := replicaPrivilegeCheck(context.Background(), r, logrus.New())
	assert.NoError(t, err) // if no replica, it returns no error.

	r.Replica, err = sql.Open("mysql", replicaDSN)
	assert.NoError(t, err) // no error
	err = replicaPrivilegeCheck(context.Background(), r, logrus.New())
	assert.NoError(t, err) // user has privileges
}

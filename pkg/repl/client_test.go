package repl

import (
	"context"
	"testing"
	"time"

	"github.com/cashapp/spirit/pkg/dbconn"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/cashapp/spirit/pkg/testutils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestReplClient(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS replt1, replt2")
	testutils.RunSQL(t, "CREATE TABLE replt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE replt2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	t1 := table.NewTableInfo(db, "test", "replt1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "replt2")
	assert.NoError(t, t2.SetInfo(t.Context()))

	logger := logrus.New()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	client := NewClient(db, cfg.Addr, t1, t2, cfg.User, cfg.Passwd, &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
	})
	assert.NoError(t, client.Run(context.Background()))
	defer client.Close()

	// Insert into t1.
	testutils.RunSQL(t, "INSERT INTO replt1 (a, b, c) VALUES (1, 2, 3)")
	assert.NoError(t, client.BlockWait(t.Context()))
	// There is no chunker attached, so the key above watermark can't apply.
	// We should observe there are now rows in the changeset.
	assert.Equal(t, 1, client.GetDeltaLen())
	assert.NoError(t, client.Flush(t.Context()))

	// We should observe there is a row in t2.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM replt2").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

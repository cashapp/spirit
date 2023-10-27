package dbconn

import (
	"testing"

	"github.com/cashapp/spirit/pkg/testutils"

	"github.com/stretchr/testify/assert"
)

func TestNewDSN(t *testing.T) {
	// Start with a basic example
	dsn := "root:password@tcp(127.0.0.1:3306)/test"
	resp, err := newDSN(dsn, NewDBConfig())
	assert.NoError(t, err)
	assert.Equal(t, "root:password@tcp(127.0.0.1:3306)/test?sql_mode=%22%22&time_zone=%22%2B00%3A00%22&innodb_lock_wait_timeout=3&lock_wait_timeout=30&transaction_isolation=%22read-committed%22&charset=binary&collation=binary", resp)

	// Also without TLS options
	dsn = "root:password@tcp(mydbhost.internal:3306)/test"
	resp, err = newDSN(dsn, NewDBConfig())
	assert.NoError(t, err)
	assert.Equal(t, "root:password@tcp(mydbhost.internal:3306)/test?sql_mode=%22%22&time_zone=%22%2B00%3A00%22&innodb_lock_wait_timeout=3&lock_wait_timeout=30&transaction_isolation=%22read-committed%22&charset=binary&collation=binary", resp) // unchanged

	// However, if it is RDS - it will be changed.
	dsn = "root:password@tcp(tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com)/test"
	resp, err = newDSN(dsn, NewDBConfig())
	assert.NoError(t, err)
	assert.Equal(t, "root:password@tcp(tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com)/test?tls=rds&sql_mode=%22%22&time_zone=%22%2B00%3A00%22&innodb_lock_wait_timeout=3&lock_wait_timeout=30&transaction_isolation=%22read-committed%22&charset=binary&collation=binary", resp)

	// This is with optional port too
	dsn = "root:password@tcp(tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com:12345)/test"
	resp, err = newDSN(dsn, NewDBConfig())
	assert.NoError(t, err)
	assert.Equal(t, "root:password@tcp(tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com:12345)/test?tls=rds&sql_mode=%22%22&time_zone=%22%2B00%3A00%22&innodb_lock_wait_timeout=3&lock_wait_timeout=30&transaction_isolation=%22read-committed%22&charset=binary&collation=binary", resp)

	// Invalid DSN, can't parse.
	dsn = "invalid"
	resp, err = newDSN(dsn, NewDBConfig())
	assert.Error(t, err)
	assert.Equal(t, "", resp)
}

func TestNewConn(t *testing.T) {
	db, err := New("invalid", NewDBConfig())
	assert.Error(t, err)
	assert.Nil(t, db)

	db, err = New(testutils.DSN(), NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	var resp int
	err = db.QueryRow("SELECT 1").Scan(&resp)
	assert.NoError(t, err)
	assert.Equal(t, 1, resp)
}

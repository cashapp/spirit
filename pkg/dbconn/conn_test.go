package dbconn

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDSN(t *testing.T) {
	// Start with a basic example
	dsn := "root:password@tcp(127.0.0.1:3306)/test"
	resp, err := newDSN(dsn)
	assert.NoError(t, err)
	assert.Equal(t, dsn, resp) // unchanged

	// Also unchanged with a hostname
	dsn = "root:password@tcp(mydbhost.internal:3306)/test"
	resp, err = newDSN(dsn)
	assert.NoError(t, err)
	assert.Equal(t, dsn, resp) // unchanged

	// However, if it is RDS - it will be changed.
	dsn = "root:password@tcp(tern-001.cluster-ro-ckeyduuwr6vm.us-west-2.rds.amazonaws.com)/test"
	resp, err = newDSN(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "root:password@tcp(tern-001.cluster-ro-ckeyduuwr6vm.us-west-2.rds.amazonaws.com)/test?tls=rds", resp)

	// This is with optional port too
	dsn = "root:password@tcp(tern-001.cluster-ro-ckeyduuwr6vm.us-west-2.rds.amazonaws.com:12345)/test"
	resp, err = newDSN(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "root:password@tcp(tern-001.cluster-ro-ckeyduuwr6vm.us-west-2.rds.amazonaws.com:12345)/test?tls=rds", resp)

	// Invalid DSN, can't parse.
	dsn = "invalid"
	resp, err = newDSN(dsn)
	assert.Error(t, err)
	assert.Equal(t, "", resp)
}

func TestNewConn(t *testing.T) {
	db, err := New("invalid")
	assert.Error(t, err)
	assert.Nil(t, db)

	db, err = New(dsn())
	assert.NoError(t, err)
	defer db.Close()

	var resp int
	err = db.QueryRow("SELECT 1").Scan(&resp)
	assert.NoError(t, err)
	assert.Equal(t, 1, resp)
}

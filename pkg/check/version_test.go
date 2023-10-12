package check

import (
	"context"
	"database/sql"
	"testing"

	"github.com/cashapp/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestVersion(t *testing.T) {
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	r := Resources{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
	}
	err = versionCheck(context.Background(), r, logrus.New())
	if utils.IsMySQL8(db) {
		assert.NoError(t, err) // all looks good of course.
	} else {
		assert.Error(t, err)
	}
}

package check

import (
	"context"
	"errors"
	"fmt"

	"github.com/cashapp/spirit/pkg/dbconn"
	"github.com/cashapp/spirit/pkg/utils"
	"github.com/siddontang/loggers"
)

func init() {
	registerCheck("version", versionCheck, ScopePreRun)
}

func versionCheck(_ context.Context, r Resources, _ loggers.Advanced) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/", r.Username, r.Password, r.Host)
	db, err := dbconn.New(dsn, dbconn.NewDBConfig())
	if err != nil {
		return err
	}
	defer db.Close()
	// This ensures that we first return an error like
	// connection refused if the host is unreachable,
	// rather than "MySQL 8.0 is required."
	if err := db.Ping(); err != nil {
		return err
	}
	if !utils.IsMySQL8(db) {
		return errors.New("MySQL 8.0 is required")
	}
	return nil
}

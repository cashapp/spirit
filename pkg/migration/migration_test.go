package migration

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/stretchr/testify/assert"
)

const (
	TestHost     = "127.0.0.1:8030"
	TestSchema   = "test"
	TestUser     = "msandbox"
	TestPassword = "msandbox"
)

func runSQL(t *testing.T, stmt string) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", TestUser, TestPassword, TestHost, TestSchema)
	db, err := sql.Open("mysql", dsn)
	assert.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(stmt)
	assert.NoError(t, err)
}

func sleep() {
	time.Sleep(50 * time.Millisecond)
}

func TestE2ENullAlter(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS t1, _t1_shadow`)
	table := `CREATE TABLE t1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	migration := &Migration{}
	migration.Host = TestHost
	migration.Username = TestUser
	migration.Password = TestPassword
	migration.Database = TestSchema
	migration.Concurrency = 16
	migration.Checksum = true
	migration.Table = "t1"
	migration.Alter = "ENGINE=InnoDB"

	err := migration.Run()
	assert.NoError(t, err)
}

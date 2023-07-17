package migration

import (
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/stretchr/testify/assert"
)

func runSQL(t *testing.T, stmt string) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(stmt)
	assert.NoError(t, err)
}

func sleep() {
	time.Sleep(50 * time.Millisecond)
}

func TestE2ENullAlterEmpty(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS t1, _t1_new`)
	table := `CREATE TABLE t1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)

	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 16
	migration.Checksum = true
	migration.Table = "t1"
	migration.Alter = "ENGINE=InnoDB"

	err = migration.Run()
	assert.NoError(t, err)
}

func TestMissingAlter(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS t1, _t1_new`)
	table := `CREATE TABLE t1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)

	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 16
	migration.Checksum = true
	migration.Table = "t1"
	migration.Alter = ""

	err = migration.Run()
	assert.Error(t, err) // missing alter
	assert.ErrorContains(t, err, "alter statement is required")
}

func TestBadDatabaseCredentials(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS t1, _t1_new`)
	table := `CREATE TABLE t1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)

	migration.Host = "127.0.0.1:9999"
	migration.Username = cfg.User
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 16
	migration.Checksum = true
	migration.Table = "t1"
	migration.Alter = "ENGINE=InnoDB"

	err = migration.Run()
	assert.Error(t, err)                                        // bad database credentials
	assert.ErrorContains(t, err, "connect: connection refused") // could be no host or temporary resolution failure.
}

func TestE2ENullAlter1Row(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS t1, _t1_new`)
	table := `CREATE TABLE t1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	runSQL(t, `insert into t1 (id,name) values (1, 'aaa')`)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)

	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 16
	migration.Checksum = true
	migration.Table = "t1"
	migration.Alter = "ENGINE=InnoDB"

	err = migration.Run()
	assert.NoError(t, err)
}

func TestE2ENullAlterWithReplicas(t *testing.T) {
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping replica tests because REPLICA_DSN not set")
	}
	runSQL(t, `DROP TABLE IF EXISTS replicatest, _replicatest_new`)
	table := `CREATE TABLE replicatest (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)

	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 16
	migration.Checksum = true
	migration.Table = "replicatest"
	migration.Alter = "ENGINE=InnoDB"
	migration.ReplicaDSN = replicaDSN
	migration.ReplicaMaxLag = 10 * time.Second

	err = migration.Run()
	assert.NoError(t, err)
}

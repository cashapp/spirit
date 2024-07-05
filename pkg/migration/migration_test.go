package migration

import (
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"

	"github.com/cashapp/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	checkpointDumpInterval = 100 * time.Millisecond
	statusInterval = 10 * time.Millisecond // the status will be accurate to 1ms
	sentinelCheckInterval = 100 * time.Millisecond
	sentinelWaitLimit = 10 * time.Second
	os.Exit(m.Run())
}

func TestE2ENullAlterEmpty(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1e2e, _t1e2e_new`)
	table := `CREATE TABLE t1e2e (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 1
	migration.Checksum = true
	migration.Table = "t1e2e"
	migration.Alter = "ENGINE=InnoDB"

	err = migration.Run()
	assert.NoError(t, err)
}

func TestMissingAlter(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1, _t1_new`)
	table := `CREATE TABLE t1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
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
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1, _t1_new`)
	table := `CREATE TABLE t1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
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
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1, _t1_new`)
	table := `CREATE TABLE t1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into t1 (id,name) values (1, 'aaa')`)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
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
	testutils.RunSQL(t, `DROP TABLE IF EXISTS replicatest, _replicatest_new`)
	table := `CREATE TABLE replicatest (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
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

// TestRenameInMySQL80 tests that even though renames are not supported,
// if the version is 8.0 it will apply the instant operation before
// the rename check applies. It's only when it needs to actually migrate
// that it won't allow renames.
func TestRenameInMySQL80(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS renamet1, _renamet1_new`)
	table := `CREATE TABLE renamet1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 16
	migration.Checksum = true
	migration.Table = "t1"
	migration.Alter = "CHANGE name nameNew varchar(255) not null"

	err = migration.Run()
	assert.NoError(t, err)
}

// TestUniqueOnNonUniqueData tests that we:
// 1. Fail trying to add a unique index on non-unique data.
// 2. The error does not blame spirit, but is instead suggestive of user-data error.
func TestUniqueOnNonUniqueData(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS uniquet1, _uniquet1_new`)
	testutils.RunSQL(t, `CREATE TABLE uniquet1 (id int not null primary key auto_increment, b int not null, pad1 varbinary(1024));`)
	testutils.RunSQL(t, `INSERT INTO uniquet1 SELECT NULL, 1, RANDOM_BYTES(1024) from dual;`)
	testutils.RunSQL(t, `INSERT INTO uniquet1 SELECT NULL, 1, RANDOM_BYTES(1024) from uniquet1 a join uniquet1 b join uniquet1 c limit 100000;`)
	testutils.RunSQL(t, `INSERT INTO uniquet1 SELECT NULL, 1, RANDOM_BYTES(1024) from uniquet1 a join uniquet1 b join uniquet1 c limit 100000;`)
	testutils.RunSQL(t, `INSERT INTO uniquet1 SELECT NULL, 1, RANDOM_BYTES(1024) from uniquet1 a join uniquet1 b join uniquet1 c limit 100000;`)
	testutils.RunSQL(t, `INSERT INTO uniquet1 SELECT NULL, 1, RANDOM_BYTES(1024) from uniquet1 a join uniquet1 b join uniquet1 c limit 100000;`)
	testutils.RunSQL(t, `UPDATE uniquet1 SET b = id;`)
	testutils.RunSQL(t, `UPDATE uniquet1 SET b = 12345 ORDER BY RAND() LIMIT 2;`)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 16
	migration.Checksum = true
	migration.Table = "uniquet1"
	migration.Alter = "ADD UNIQUE (b)"
	err = migration.Run()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "Check that the ALTER statement is not adding a UNIQUE INDEX to non-unique data")
}

func TestGeneratedColumns(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1generated, _t1generated_new`)
	table := `CREATE TABLE t1generated (
	 id int not null primary key auto_increment,
    b int not null,
    c int GENERATED ALWAYS AS  (b + 1)
	)`
	testutils.RunSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 1
	migration.Checksum = true
	migration.Table = "t1generated"
	migration.Alter = "ENGINE=InnoDB"

	err = migration.Run()
	assert.NoError(t, err)
}

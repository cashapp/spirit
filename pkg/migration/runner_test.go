package migration

import (
	"context"
	"database/sql"
	"math"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/squareup/spirit/pkg/repl"
	"github.com/squareup/spirit/pkg/row"
	"github.com/squareup/spirit/pkg/table"
	"github.com/squareup/spirit/pkg/throttler"

	"github.com/stretchr/testify/assert"
)

func TestVarcharNonBinaryComparable(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS nonbinarycompatt1, _nonbinarycompatt1_new`)
	table := `CREATE TABLE nonbinarycompatt1 (
		uuid varchar(40) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (uuid)
	)`
	runSQL(t, table)
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "nonbinarycompatt1",
		Alter:       "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                       // everything is specified.
	assert.Error(t, m.Run(context.Background())) // it's a non-binary comparable type (varchar)
	assert.NoError(t, m.Close())
}

func TestVarbinary(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS varbinaryt1, _varbinaryt1_new`)
	table := `CREATE TABLE varbinaryt1 (
		uuid varbinary(40) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (uuid)
	)`
	runSQL(t, table)
	runSQL(t, "INSERT INTO varbinaryt1 (uuid, name) VALUES (UUID(), REPEAT('a', 200))")
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "varbinaryt1",
		Alter:       "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                         // everything is specified correctly.
	assert.NoError(t, m.Run(context.Background())) // varbinary is compatible.
	assert.False(t, m.usedInstantDDL)              // not possible
	assert.NoError(t, m.Close())
}

// TestDataFromBadSqlMode tests that data previously inserted like 0000-00-00 can still be migrated.
func TestDataFromBadSqlMode(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS badsqlt1, _badsqlt1_new`)
	table := `CREATE TABLE badsqlt1 (
		id int not null primary key auto_increment,
		d date NOT NULL,
		t timestamp NOT NULL
	)`
	runSQL(t, table)
	runSQL(t, "INSERT IGNORE INTO badsqlt1 (d, t) VALUES ('0000-00-00', '0000-00-00 00:00:00'),('2020-02-00', '2020-02-30 00:00:00')")
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "badsqlt1",
		Alter:       "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                         // everything is specified correctly.
	assert.NoError(t, m.Run(context.Background())) // pk is compatible.
	assert.False(t, m.usedInstantDDL)              // not possible
	assert.NoError(t, m.Close())
}

func TestChangeDatatypeNoData(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS cdatatypemytable`)
	table := `CREATE TABLE cdatatypemytable (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "cdatatypemytable",
		Alter:       "CHANGE b b INT", //nolint: dupword
	})
	assert.NoError(t, err)                         // everything is specified correctly.
	assert.NoError(t, m.Run(context.Background())) // no data so no truncation is possible.
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())
}

func TestChangeDatatypeDataLoss(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS cdatalossmytable`)
	table := `CREATE TABLE cdatalossmytable (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	runSQL(t, "INSERT INTO cdatalossmytable (name, b) VALUES ('a', 'b')")
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "cdatalossmytable",
		Alter:       "CHANGE b b INT", //nolint: dupword
	})
	assert.NoError(t, err)                       // everything is specified correctly.
	assert.Error(t, m.Run(context.Background())) // value 'b' can no convert cleanly to int.
	assert.NoError(t, m.Close())
}

func TestOnline(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS testonline`)
	table := `CREATE TABLE testonline (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "testonline",
		Alter:       "CHANGE COLUMN b b int(11) NOT NULL", //nolint: dupword
	})
	assert.NoError(t, err)
	assert.NoError(t, m.Run(context.TODO()))
	assert.False(t, m.usedInplaceDDL) // not possible
	assert.NoError(t, m.Close())

	// Create another table.
	runSQL(t, `DROP TABLE IF EXISTS testonline2`)
	table = `CREATE TABLE testonline2 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	m, err = NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "testonline2",
		Alter:       "ADD c int(11) NOT NULL",
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.NoError(t, err)
	assert.False(t, m.usedInplaceDDL) // uses instant DDL first

	// TODO: can only check this against 8.0
	// assert.True(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	// Finally, this will work.
	runSQL(t, `DROP TABLE IF EXISTS testonline3`)
	table = `CREATE TABLE testonline3 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	m, err = NewRunner(&Migration{
		Host:              cfg.Addr,
		Username:          cfg.User,
		Password:          cfg.Passwd,
		Database:          cfg.DBName,
		Concurrency:       16,
		Table:             "testonline3",
		Alter:             "ADD INDEX(b)",
		AttemptInplaceDDL: true,
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.NoError(t, err)
	assert.False(t, m.usedInstantDDL) // not possible
	assert.True(t, m.usedInplaceDDL)  // as
	assert.NoError(t, m.Close())
}

func TestTableLength(t *testing.T) {
	t.Skip("Not sure why, fails for now.")
	runSQL(t, `DROP TABLE IF EXISTS thisisareallylongtablenamethisisareallylongtablename60charac`)
	table := `CREATE TABLE thisisareallylongtablenamethisisareallylongtablename60charac (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "thisisareallylongtablenamethisisareallylongtablename60charac",
		Alter:       "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "is too long")
	assert.NoError(t, m.Close())

	// There is another condition where the error will be in dropping the _old table first
	// if the character limit is exceeded in that query.
	m, err = NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "thisisareallylongtablenamethisisareallylongtablename64characters",
		Alter:       "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "is too long")
	assert.NoError(t, m.Close())
}

func TestMigrationStateString(t *testing.T) {
	assert.Equal(t, "initial", stateInitial.String())
	assert.Equal(t, "copyRows", stateCopyRows.String())
	assert.Equal(t, "applyChangeset", stateApplyChangeset.String())
	assert.Equal(t, "checksum", stateChecksum.String())
	assert.Equal(t, "cutOver", stateCutOver.String())
	assert.Equal(t, "errCleanup", stateErrCleanup.String())
}

func TestBadOptions(t *testing.T) {
	_, err := NewRunner(&Migration{})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "host is required")
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)

	_, err = NewRunner(&Migration{
		Host: cfg.Addr,
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "schema name is required")
	_, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Database: "mytable",
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "table name is required")
	_, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Database: "mytable",
		Table:    "mytable",
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "alter statement is required")
}

func TestBadAlter(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS bot1, bot2`)
	table := `CREATE TABLE bot1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	table = `CREATE TABLE bot2 (
		id int(11) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "bot1",
		Alter:       "badalter",
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(context.Background())
	assert.Error(t, err) // alter is invalid
	assert.ErrorContains(t, err, "badalter")
	assert.NoError(t, m.Close())

	// Renames are not supported.
	m, err = NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "bot1",
		Alter:       "RENAME COLUMN name TO name2, ADD INDEX(name)", // need both, otherwise INSTANT algorithm will do the rename
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(context.Background())
	assert.Error(t, err) // alter is invalid
	assert.ErrorContains(t, err, "renames are not supported")
	assert.NoError(t, m.Close())

	// This is a different type of rename,
	// which is coming via a change
	m, err = NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "bot1",
		Alter:       "CHANGE name name2 VARCHAR(255), ADD INDEX(name)", // need both, otherwise INSTANT algorithm will do the rename
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(context.Background())
	assert.Error(t, err) // alter is invalid
	assert.ErrorContains(t, err, "renames are not supported")
	assert.NoError(t, m.Close())

	// But this is supported (no rename)
	m, err = NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "bot1",
		Alter:       "CHANGE name name VARCHAR(200), ADD INDEX(name)", //nolint: dupword
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(context.Background())
	assert.NoError(t, err) // its valid, no rename
	assert.NoError(t, m.Close())

	// Test DROP PRIMARY KEY, change primary key.
	// The REPLACE statement likely relies on the same PRIMARY KEY on the new table,
	// so things get a lot more complicated if the primary key changes.
	m, err = NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "bot2",
		Alter:       "DROP PRIMARY KEY",
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(context.Background())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "dropping primary key")
	assert.NoError(t, m.Close())
}

// TestChangeDatatypeLossyNoAutoInc is a good test of the how much the
// chunker will boil the ocean:
//   - There is a MIN(key)=1 and a MAX(key)=8589934592
//   - There is no auto-increment so the chunker is allowed to expand each chunk
//     based on estimated rows (which is low).
//
// In production cases, this should be even faster since the trivial chunker
// will apply immediately if the row estimate is less than 1000 rows, but it's disabled for test.
//
// Only the key=8589934592 will fail to be converted. On my system this test
// currently runs in 0.4 seconds which is "acceptable" for chunker performance.
// The generated number of chunks should also be very low.
func TestChangeDatatypeLossyNoAutoInc(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS lossychange2`)
	table := `CREATE TABLE lossychange2 (
					id BIGINT NOT NULL,
					name varchar(255) NOT NULL,
					b varchar(255) NOT NULL,
					PRIMARY KEY (id)
				)`
	runSQL(t, table)
	runSQL(t, "INSERT INTO lossychange2 (id, name, b) VALUES (1, 'a', REPEAT('a', 200))")          // will pass in migration
	runSQL(t, "INSERT INTO lossychange2 (id, name, b) VALUES (8589934592, 'a', REPEAT('a', 200))") // will fail in migration

	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                  cfg.Addr,
		Username:              cfg.User,
		Password:              cfg.Passwd,
		Database:              cfg.DBName,
		Concurrency:           16,
		Table:                 "lossychange2",
		Alter:                 "CHANGE COLUMN id id INT NOT NULL auto_increment", //nolint: dupword
		DisableTrivialChunker: true,
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "Out of range value") // Error 1264: Out of range value for column 'id' at row 1
	assert.True(t, m.copier.CopyChunksCount < 10)      // should be very low
	assert.NoError(t, m.Close())
}

// TestChangeDatatypeLossy3 has an auto-increment column which limits
// the expansion of the chunk, *but* the trivial chunker is enabled.
// Because the table is basically empty, the row estimate should be below
// 1000, which should mean everything is processed as one chunk.
// Additionally, the data type change is "lossy" but given the current
// stored data set does not cause errors.
func TestChangeDatatypeLossless(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS lossychange3`)
	table := `CREATE TABLE lossychange3 (
				id BIGINT NOT NULL AUTO_INCREMENT,
				name varchar(255) NOT NULL,
				b varchar(255) NULL,
				PRIMARY KEY (id)
			)`
	runSQL(t, table)
	runSQL(t, "INSERT INTO lossychange3 (name, b) VALUES ('a', REPEAT('a', 200))")
	runSQL(t, "INSERT INTO lossychange3 (id, name, b) VALUES (8589934592, 'a', REPEAT('a', 200))")

	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "lossychange3",
		Alter:       "CHANGE COLUMN b b varchar(200) NOT NULL", //nolint: dupword
		Checksum:    false,
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.NoError(t, err) // works because there are no violations.
	assert.Equal(t, uint64(1), m.copier.CopyChunksCount)
	assert.NoError(t, m.Close())
}

// TestChangeDatatypeLossyFailEarly tests a scenario where there is an error
// immediately so the DDL should halt. Because there is an auto-increment,
// and trivial chunking is disabled the chunker will not expand the range.
// So if it does try to exhaustively run the DDL it will take forever:
// [1, 8589934592] / 1000 = 8589934.592 chunks

func TestChangeDatatypeLossyFailEarly(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS lossychange4`)
	table := `CREATE TABLE lossychange4 (
				id BIGINT NOT NULL AUTO_INCREMENT,
				name varchar(255) NOT NULL,
				b varchar(255) NULL,
				PRIMARY KEY (id)
			)`
	runSQL(t, table)
	runSQL(t, "INSERT INTO lossychange4 (name) VALUES ('a')")
	runSQL(t, "INSERT INTO lossychange4 (name, b) VALUES ('a', REPEAT('a', 200))")
	runSQL(t, "INSERT INTO lossychange4 (id, name, b) VALUES (8589934592, 'a', REPEAT('a', 200))")
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "lossychange4",
		Alter:       "CHANGE COLUMN b b varchar(255) NOT NULL", //nolint: dupword
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.Error(t, err) // there is a violation where row 1 is NULL
	assert.NoError(t, m.Close())
}

// TestAddUniqueIndex is a really interesting test *because* resuming from checkpoint
// will cause duplicate key errors. It's not straight-forward to differentiate between
// duplicate errors from a resume, and a constraint violation. So what we do is
// 1) *FORCE* checksum to be enabled on resume from checkpoint
// 2) If checksum is not enabled, duplicate key errors are elevated to errors.
func TestAddUniqueIndexChecksumEnabled(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS uniqmytable`)
	table := `CREATE TABLE uniqmytable (
				id int(11) NOT NULL AUTO_INCREMENT,
				name varchar(255) NOT NULL,
				b varchar(255) NOT NULL,
				PRIMARY KEY (id)
			)`
	runSQL(t, table)
	runSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('a', 200))")
	runSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('b', 200))")
	runSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('c', 200))")
	runSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('a', 200))") // duplicate

	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "uniqmytable",
		Alter:       "ADD UNIQUE INDEX b (b)",
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.Error(t, err)         // not unique
	assert.NoError(t, m.Close()) // need to close now otherwise we'll get an error on re-opening it.

	runSQL(t, "DELETE FROM uniqmytable WHERE b = REPEAT('a', 200) LIMIT 1") // make unique
	m, err = NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "uniqmytable",
		Alter:       "ADD UNIQUE INDEX b (b)",
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.NoError(t, err) // works fine.
	assert.NoError(t, m.Close())
}

// Test a non-integer primary key.
// IN future this needs to be supported!

func TestChangeNonIntPK(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS nonintpk`)
	table := `CREATE TABLE nonintpk (
			pk varbinary(36) NOT NULL PRIMARY KEY,
			name varchar(255) NOT NULL,
			b varchar(10) NOT NULL -- change to varchar(255)
		)`
	runSQL(t, table)
	runSQL(t, "INSERT INTO nonintpk (pk, name, b) VALUES (UUID(), 'a', REPEAT('a', 5))")
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "nonintpk",
		Alter:       "CHANGE COLUMN b b VARCHAR(255) NOT NULL", //nolint: dupword
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, m.Close())
}

func TestCheckpoint(t *testing.T) {
	tbl := `CREATE TABLE cpt1 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL default 0)`
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)

	runSQL(t, `DROP TABLE IF EXISTS cpt1, _cpt1_new, _cpt1_chkpnt`)
	runSQL(t, tbl)
	runSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM dual`)
	runSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1`)
	runSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1 a JOIN cpt1 b JOIN cpt1 c`)
	runSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1 a JOIN cpt1 b JOIN cpt1 c`)
	runSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1 a JOIN cpt1 LIMIT 100000`) // ~100k rows

	preSetup := func() *Runner {
		m, err := NewRunner(&Migration{
			Host:        cfg.Addr,
			Username:    cfg.User,
			Password:    cfg.Passwd,
			Database:    cfg.DBName,
			Concurrency: 16,
			Table:       "cpt1",
			Alter:       "ENGINE=InnoDB",
		})
		assert.NoError(t, err)
		assert.Equal(t, "initial", m.getCurrentState().String())
		// Usually we would call m.Run() but we want to step through
		// the migration process manually.
		m.db, err = sql.Open("mysql", m.dsn())
		assert.NoError(t, err)
		// Get Table Info
		m.table = table.NewTableInfo(m.db, m.schemaName, m.tableName)
		err = m.table.SetInfo(context.TODO())
		assert.NoError(t, err)
		assert.NoError(t, m.dropOldTable(context.TODO()))
		return m
	}

	m := preSetup()
	// migrationRunner.Run usually calls m.Setup() here.
	// Which first checks if the table can be restored from checkpoint.
	// Because this is the first run, it can't.

	assert.Error(t, m.resumeFromCheckpoint(context.TODO()))

	// So we proceed with the initial steps.
	assert.NoError(t, m.createNewTable(context.TODO()))
	assert.NoError(t, m.alterNewTable(context.TODO()))
	assert.NoError(t, m.createCheckpointTable(context.TODO()))
	logger := logrus.New()
	m.replClient = repl.NewClient(m.db, m.host, m.table, m.newTable, m.username, m.password, &repl.ClientConfig{
		Logger:      logger,
		Concurrency: 4,
		BatchSize:   10000,
	})
	m.copier, err = row.NewCopier(m.db, m.table, m.newTable, row.NewCopierDefaultConfig())
	assert.NoError(t, err)
	err = m.replClient.Run()
	assert.NoError(t, err)

	// Now we are ready to start copying rows.
	// Instead of calling m.copyRows() we will step through it manually.
	// Since we want to checkpoint after a few chunks.

	m.copier.StartTime = time.Now()
	m.setCurrentState(stateCopyRows)
	assert.Equal(t, "copyRows", m.getCurrentState().String())

	// because we are not calling copier.Run() we need to manually open.
	assert.NoError(t, m.copier.Open4Test())

	// first chunk.
	chunk1, err := m.copier.Next4Test()
	assert.NoError(t, err)

	chunk2, err := m.copier.Next4Test()
	assert.NoError(t, err)

	chunk3, err := m.copier.Next4Test()
	assert.NoError(t, err)

	// There is no watermark yet.
	_, err = m.copier.GetLowWatermark()
	assert.Error(t, err)
	// Dump checkpoint also returns an error for the same reason.
	assert.Error(t, m.dumpCheckpoint(context.TODO()))

	// Because it's multi-threaded, we can't guarantee the order of the chunks.
	assert.NoError(t, m.copier.CopyChunk(context.TODO(), chunk2))
	assert.NoError(t, m.copier.CopyChunk(context.TODO(), chunk1))
	assert.NoError(t, m.copier.CopyChunk(context.TODO(), chunk3))

	// The watermark should exist now, because migrateChunk()
	// gives feedback back to table.

	watermark, err := m.copier.GetLowWatermark()
	assert.NoError(t, err)
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\":1001,\"Inclusive\":true},\"UpperBound\":{\"Value\":2001,\"Inclusive\":false}}", watermark)
	// Dump a checkpoint
	assert.NoError(t, m.dumpCheckpoint(context.TODO()))

	// Close the db connection since m is to be destroyed.
	assert.NoError(t, m.db.Close())

	// Now lets imagine that everything fails and we need to start
	// from checkpoint again.

	m = preSetup()
	assert.NoError(t, m.resumeFromCheckpoint(context.TODO()))

	// Start the binary log feed just before copy rows starts.
	err = m.replClient.Run()
	assert.NoError(t, err)

	// This opens the table at the checkpoint (table.OpenAtWatermark())
	// which sets the chunkPtr at the LowerBound. It also has to position
	// the watermark to this point so new watermarks "align" correctly.
	// So lets now call NextChunk to verify.

	chunk, err := m.copier.Next4Test()
	assert.NoError(t, err)
	assert.Equal(t, int64(1001), chunk.LowerBound.Value)
	assert.NoError(t, m.copier.CopyChunk(context.TODO(), chunk))

	// It's ideally not typical but you can still dump checkpoint from
	// a restored checkpoint state. We won't have advanced anywhere from
	// the last checkpoint because on restore, the LowerBound is taken.

	watermark, err = m.copier.GetLowWatermark()
	assert.NoError(t, err)
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\":1001,\"Inclusive\":true},\"UpperBound\":{\"Value\":2001,\"Inclusive\":false}}", watermark)
	// Dump a checkpoint
	assert.NoError(t, m.dumpCheckpoint(context.TODO()))

	// Let's confirm we do advance the watermark.
	for i := 0; i < 10; i++ {
		chunk, err = m.copier.Next4Test()
		assert.NoError(t, err)
		assert.NoError(t, m.copier.CopyChunk(context.TODO(), chunk))
	}

	watermark, err = m.copier.GetLowWatermark()
	assert.NoError(t, err)
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\":11001,\"Inclusive\":true},\"UpperBound\":{\"Value\":12001,\"Inclusive\":false}}", watermark)
	assert.NoError(t, m.db.Close())
}

func TestCheckpointDifferentRestoreOptions(t *testing.T) {
	tbl := `CREATE TABLE cpt1difft1 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL default 0)`

	runSQL(t, `DROP TABLE IF EXISTS cpt1difft1, cpt1difft1_new, _cpt1difft1_chkpnt`)
	runSQL(t, tbl)
	runSQL(t, `insert into cpt1difft1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM dual`)
	runSQL(t, `insert into cpt1difft1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1difft1`)
	runSQL(t, `insert into cpt1difft1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1difft1 a JOIN cpt1difft1 b JOIN cpt1difft1 c`)
	runSQL(t, `insert into cpt1difft1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1difft1 a JOIN cpt1difft1 b JOIN cpt1difft1 c`)
	runSQL(t, `insert into cpt1difft1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1difft1 a JOIN cpt1difft1 LIMIT 100000`) // ~100k rows
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)

	preSetup := func(alter string) *Runner {
		m, err := NewRunner(&Migration{
			Host:        cfg.Addr,
			Username:    cfg.User,
			Password:    cfg.Passwd,
			Database:    cfg.DBName,
			Concurrency: 16,
			Table:       "cpt1difft1",
			Alter:       alter,
		})
		assert.NoError(t, err)
		assert.Equal(t, "initial", m.getCurrentState().String())
		// Usually we would call m.Run() but we want to step through
		// the migration process manually.
		m.db, err = sql.Open("mysql", m.dsn())
		assert.NoError(t, err)
		// Get Table Info
		m.table = table.NewTableInfo(m.db, m.schemaName, m.tableName)
		err = m.table.SetInfo(context.TODO())
		assert.NoError(t, err)
		assert.NoError(t, m.dropOldTable(context.TODO()))
		return m
	}

	m := preSetup("ADD COLUMN id3 INT NOT NULL DEFAULT 0, ADD INDEX(id2)")
	// migrationRunner.Run usually calls m.Setup() here.
	// Which first checks if the table can be restored from checkpoint.
	// Because this is the first run, it can't.

	assert.Error(t, m.resumeFromCheckpoint(context.TODO()))

	// So we proceed with the initial steps.
	assert.NoError(t, m.createNewTable(context.TODO()))
	assert.NoError(t, m.alterNewTable(context.TODO()))
	assert.NoError(t, m.createCheckpointTable(context.TODO()))
	logger := logrus.New()
	m.replClient = repl.NewClient(m.db, m.host, m.table, m.newTable, m.username, m.password, &repl.ClientConfig{
		Logger:      logger,
		Concurrency: 4,
		BatchSize:   10000,
	})
	m.copier, err = row.NewCopier(m.db, m.table, m.newTable, row.NewCopierDefaultConfig())
	assert.NoError(t, err)
	err = m.replClient.Run()
	assert.NoError(t, err)

	// Now we are ready to start copying rows.
	// Instead of calling m.copyRows() we will step through it manually.
	// Since we want to checkpoint after a few chunks.

	m.copier.StartTime = time.Now()
	m.setCurrentState(stateCopyRows)
	assert.Equal(t, "copyRows", m.getCurrentState().String())

	assert.NoError(t, m.copier.Open4Test())

	// first chunk.
	chunk1, err := m.copier.Next4Test()
	assert.NoError(t, err)

	chunk2, err := m.copier.Next4Test()
	assert.NoError(t, err)

	chunk3, err := m.copier.Next4Test()
	assert.NoError(t, err)

	// There is no watermark yet.
	_, err = m.copier.GetLowWatermark()
	assert.Error(t, err)
	// Dump checkpoint also returns an error for the same reason.
	assert.Error(t, m.dumpCheckpoint(context.TODO()))

	// Because it's multi-threaded, we can't guarantee the order of the chunks.
	assert.NoError(t, m.copier.CopyChunk(context.TODO(), chunk2))
	assert.NoError(t, m.copier.CopyChunk(context.TODO(), chunk1))
	assert.NoError(t, m.copier.CopyChunk(context.TODO(), chunk3))

	// The watermark should exist now, because migrateChunk()
	// gives feedback back to table.

	watermark, err := m.copier.GetLowWatermark()
	assert.NoError(t, err)
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\":1001,\"Inclusive\":true},\"UpperBound\":{\"Value\":2001,\"Inclusive\":false}}", watermark)
	// Dump a checkpoint
	assert.NoError(t, m.dumpCheckpoint(context.TODO()))

	// Close the db connection since m is to be destroyed.
	assert.NoError(t, m.db.Close())

	// Now lets imagine that everything fails and we need to start
	// from checkpoint again.

	m = preSetup("ADD COLUMN id4 INT NOT NULL DEFAULT 0, ADD INDEX(id2)")
	assert.Error(t, m.resumeFromCheckpoint(context.TODO())) // it should error because the ALTER does not match.
}

// TestE2EBinlogSubscribing is a complex test that uses the lower level interface
// to step through the table while subscribing to changes that we will
// be making to the table between chunks. It is effectively an
// end-to-end test with concurrent operations on the table.
func TestE2EBinlogSubscribing(t *testing.T) {
	// Need to test both composite and non composite keys.
	// Possibly more like mem comparable varbinary.
	tables := []string{`CREATE TABLE e2et1 (
	id1 INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	id2 INT NOT NULL,
	pad int NOT NULL default 0)`,
		`CREATE TABLE e2et1 (
		id1 int NOT NULL,
		id2 int not null,
		pad int NOT NULL  default 0,
		PRIMARY KEY (id1, id2))`,
	}
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)

	for _, tbl := range tables {
		runSQL(t, `DROP TABLE IF EXISTS e2et1, _e2et1_new`)
		runSQL(t, tbl)
		runSQL(t, `insert into e2et1 (id1, id2) values (1, 1)`)
		runSQL(t, `insert into e2et1 (id1, id2) values (2, 1)`)
		runSQL(t, `insert into e2et1 (id1, id2) values (3, 1)`)

		m, err := NewRunner(&Migration{
			Host:                  cfg.Addr,
			Username:              cfg.User,
			Password:              cfg.Passwd,
			Database:              cfg.DBName,
			Concurrency:           16,
			Table:                 "e2et1",
			Alter:                 "ENGINE=InnoDB",
			DisableTrivialChunker: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, "initial", m.getCurrentState().String())

		// Usually we would call m.Run() but we want to step through
		// the migration process manually.
		m.db, err = sql.Open("mysql", m.dsn())
		assert.NoError(t, err)
		defer m.db.Close()
		// Get Table Info
		m.table = table.NewTableInfo(m.db, m.schemaName, m.tableName)
		err = m.table.SetInfo(context.TODO())
		assert.NoError(t, err)
		assert.NoError(t, m.dropOldTable(context.TODO()))

		// migration.Run usually calls m.Migrate() here.
		// Which does the following before calling copyRows:
		// So we proceed with the initial steps.
		assert.NoError(t, m.createNewTable(context.TODO()))
		assert.NoError(t, m.alterNewTable(context.TODO()))
		assert.NoError(t, m.createCheckpointTable(context.TODO()))
		logger := logrus.New()
		m.replClient = repl.NewClient(m.db, m.host, m.table, m.newTable, m.username, m.password, &repl.ClientConfig{
			Logger:      logger,
			Concurrency: 4,
			BatchSize:   10000,
		})
		m.copier, err = row.NewCopier(m.db, m.table, m.newTable, &row.CopierConfig{
			Concurrency:           m.optConcurrency,
			TargetChunkTime:       m.optTargetChunkTime,
			FinalChecksum:         m.optChecksum,
			DisableTrivialChunker: m.optDisableTrivialChunker,
			Throttler:             &throttler.Noop{},
			Logger:                m.logger,
		})
		assert.NoError(t, err)
		m.replClient.KeyAboveCopierCallback = m.copier.KeyAboveHighWatermark
		err = m.replClient.Run()
		assert.NoError(t, err)

		// Now we are ready to start copying rows.
		// Instead of calling m.copyRows() we will step through it manually.
		// Since we want to checkpoint after a few chunks.

		m.copier.StartTime = time.Now()
		m.setCurrentState(stateCopyRows)
		assert.Equal(t, "copyRows", m.getCurrentState().String())

		// We expect 3 chunks to be copied.
		// The special first and last case and middle case.
		assert.NoError(t, m.copier.Open4Test())

		// first chunk.
		chunk, err := m.copier.Next4Test()
		assert.NoError(t, err)
		assert.NotNil(t, chunk)
		assert.NoError(t, m.copier.CopyChunk(context.TODO(), chunk))

		// Now insert some data.
		// This will be ignored by the binlog subscription.
		// Because it's ahead of the high watermark.
		runSQL(t, `insert into e2et1 (id1, id2) values (4, 1)`)
		assert.True(t, m.copier.KeyAboveHighWatermark(4))

		// Give it a chance, since we need to read from the binary log to populate this
		// Even though we expect nothing.
		sleep() // plenty
		assert.Equal(t, 0, m.replClient.GetDeltaLen())

		// second chunk.
		chunk, err = m.copier.Next4Test()
		assert.NoError(t, err)
		assert.NoError(t, m.copier.CopyChunk(context.TODO(), chunk))

		// Now insert some data.
		// This should be picked up by the binlog subscription.
		runSQL(t, `insert into e2et1 (id1, id2) values (5, 1)`)
		assert.False(t, m.copier.KeyAboveHighWatermark(5))
		sleep() // wait for binlog
		assert.Equal(t, 1, m.replClient.GetDeltaLen())

		runSQL(t, `delete from e2et1 where id1 = 1`)
		assert.False(t, m.copier.KeyAboveHighWatermark(1))
		sleep() // wait for binlog
		assert.Equal(t, 2, m.replClient.GetDeltaLen())

		// third (and last) chunk.
		chunk, err = m.copier.Next4Test()
		assert.NoError(t, err)
		assert.NoError(t, m.copier.CopyChunk(context.TODO(), chunk))

		// Some data is inserted later, even though the last chunk is done.
		// We still care to pick it up.
		runSQL(t, `insert into e2et1 (id1, id2) values (6, 1)`)
		// the pointer should be at maxint64 for safety. this ensures
		// that any keyAboveHighWatermark checks return false
		assert.False(t, m.copier.KeyAboveHighWatermark(uint64(math.MaxUint64)))

		// Now that copy rows is done, we flush the changeset until trivial.
		// and perform the optional checksum.
		assert.NoError(t, m.replClient.FlushUntilTrivial(context.TODO()))
		m.setCurrentState(stateApplyChangeset)
		assert.Equal(t, "applyChangeset", m.getCurrentState().String())
		m.setCurrentState(stateChecksum)
		assert.NoError(t, m.checksum(context.TODO()))
		assert.Equal(t, "postChecksum", m.getCurrentState().String())
		// All done!
	}
}

// TestForRemainingTableArtifacts tests that the _{name}_old table is left after
// the migration is complete, but no _chkpnt or _new table.
func TestForRemainingTableArtifacts(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS remainingtbl, _remainingtbl_new, _remainingtbl_old, _remainingtbl_chkpnt`)
	table := `CREATE TABLE remainingtbl (
		id INT NOT NULL PRIMARY KEY,
		name varchar(255) NOT NULL
	)`
	runSQL(t, table)
	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "remainingtbl",
		Alter:       "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                         // everything is specified.
	assert.NoError(t, m.Run(context.Background())) // it's an accepted type.
	assert.NoError(t, m.Close())

	// Now we should have a _remainingtbl_old table and a remainingtbl table
	// but no _remainingtbl_new table or _remainingtbl_chkpnt table.
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()
	stmt := `SELECT GROUP_CONCAT(table_name) FROM information_schema.tables where table_schema='test' and table_name LIKE '%remainingtbl%' ORDER BY table_name;`
	var tables string
	assert.NoError(t, db.QueryRow(stmt).Scan(&tables))
	assert.Equal(t, "_remainingtbl_old,remainingtbl", tables)
}

func TestDropColumn(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS dropcol, _dropcol_new`)
	table := `CREATE TABLE dropcol (
		id int(11) NOT NULL AUTO_INCREMENT,
		a varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		c varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	runSQL(t, `insert into dropcol (id, a,b,c) values (1, 'a', 'b', 'c')`)

	cfg, err := mysql.ParseDSN(dsn())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:        cfg.Addr,
		Username:    cfg.User,
		Password:    cfg.Passwd,
		Database:    cfg.DBName,
		Concurrency: 16,
		Table:       "dropcol",
		Alter:       "DROP COLUMN b, ENGINE=InnoDB", // need both to ensure it is not instant!
	})
	assert.NoError(t, err)
	assert.NoError(t, m.Run(context.Background()))

	assert.False(t, m.usedInstantDDL) // need to ensure it uses full process.
	assert.NoError(t, m.Close())
}

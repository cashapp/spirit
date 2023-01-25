package migration

import (
	"context"
	"database/sql"
	"math"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/squareup/gap-core/log"
	"github.com/squareup/spirit/pkg/copier"
	"github.com/squareup/spirit/pkg/repl"
	"github.com/squareup/spirit/pkg/table"

	"github.com/stretchr/testify/assert"
)

func TestVarcharNonBinaryComparable(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS t1, _t1_shadow`)
	table := `CREATE TABLE t1 (
		uuid varchar(40) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (uuid)
	)`
	runSQL(t, table)

	m, err := NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "t1",
		Alter:       "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                       // everything is specified.
	assert.Error(t, m.Run(context.Background())) // it's a non-binary comparable type (varchar)
}

func TestVarbinary(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS t1, _t1_shadow`)
	table := `CREATE TABLE t1 (
		uuid varbinary(40) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (uuid)
	)`
	runSQL(t, table)
	runSQL(t, "INSERT INTO t1 (uuid, name) VALUES (UUID(), REPEAT('a', 200))")
	m, err := NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "t1",
		Alter:       "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                         // everything is specified correctly.
	assert.NoError(t, m.Run(context.Background())) // varbinary is compatible.
	assert.False(t, m.usedInstantDDL)              // not possible
	assert.NoError(t, m.Close())
}

func TestChangeDatatypeNoData(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS mytable`)
	table := `CREATE TABLE mytable (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	m, err := NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "mytable",
		Alter:       "CHANGE b b INT", //nolint: dupword
	})
	assert.NoError(t, err)                         // everything is specified correctly.
	assert.NoError(t, m.Run(context.Background())) // no data so no truncation is possible.
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())
}

func TestChangeDatatypeDataLoss(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS mytable`)
	table := `CREATE TABLE mytable (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	runSQL(t, "INSERT INTO mytable (name, b) VALUES ('a', 'b')")
	m, err := NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "mytable",
		Alter:       "CHANGE b b INT", //nolint: dupword
	})
	assert.NoError(t, err)                       // everything is specified correctly.
	assert.Error(t, m.Run(context.Background())) // value 'b' can no convert cleanly to int.
}

func TestOnline(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS mytable`)
	table := `CREATE TABLE mytable (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	m, err := NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "mytable",
		Alter:       "CHANGE COLUMN b b int(11) NOT NULL", //nolint: dupword
	})
	assert.NoError(t, err)
	assert.NoError(t, m.Run(context.TODO()))
	assert.False(t, m.usedInplaceDDL) // not possible

	// Create another table.
	runSQL(t, `DROP TABLE IF EXISTS mytable2`)
	table = `CREATE TABLE mytable2 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	m, err = NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "mytable2",
		Alter:       "ADD c int(11) NOT NULL",
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.NoError(t, err)
	assert.False(t, m.usedInplaceDDL) // uses instant DDL first
	assert.True(t, m.usedInstantDDL)

	// Finally, this will work.
	runSQL(t, `DROP TABLE IF EXISTS mytable3`)
	table = `CREATE TABLE mytable3 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	m, err = NewMigrationRunner(&Migration{
		Host:              TestHost,
		Username:          TestUser,
		Password:          TestPassword,
		Database:          TestSchema,
		Concurrency:       16,
		Table:             "mytable3",
		Alter:             "ADD INDEX(b)",
		AttemptInplaceDDL: true,
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.NoError(t, err)
	assert.False(t, m.usedInstantDDL) // not possible
	assert.True(t, m.usedInplaceDDL)  // as requested
}

func TestTableLength(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS thisisareallylongtablenamethisisareallylongtablename60charac`)
	table := `CREATE TABLE thisisareallylongtablenamethisisareallylongtablename60charac (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	m, err := NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "thisisareallylongtablenamethisisareallylongtablename60charac",
		Alter:       "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "is too long")

	// There is another condition where the error will be in dropping the _old table first
	// if the character limit is exceeded in that query.
	m, err = NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "thisisareallylongtablenamethisisareallylongtablename64characters",
		Alter:       "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "is too long")
}

func TestMigrationStateString(t *testing.T) {
	assert.Equal(t, "initial", migrationStateInitial.String())
	assert.Equal(t, "copyRows", migrationStateCopyRows.String())
	assert.Equal(t, "applyChangeset", migrationStateApplyChangeset.String())
	assert.Equal(t, "checksum", migrationStateChecksum.String())
	assert.Equal(t, "cutOver", migrationStateCutOver.String())
	assert.Equal(t, "errCleanup", migrationStateErrCleanup.String())
}

func TestBadOptions(t *testing.T) {
	_, err := NewMigrationRunner(&Migration{})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "host is required")

	_, err = NewMigrationRunner(&Migration{
		Host: TestHost,
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "schema name is required")
	_, err = NewMigrationRunner(&Migration{
		Host:     TestHost,
		Database: "mytable",
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "table name is required")
	_, err = NewMigrationRunner(&Migration{
		Host:     TestHost,
		Database: "mytable",
		Table:    "mytable",
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "alter statement is required")
}

func TestBadAlter(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS t1`)
	table := `CREATE TABLE t1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	m, err := NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "t1",
		Alter:       "badalter",
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(context.Background())
	assert.Error(t, err) // alter is invalid
	assert.ErrorContains(t, err, "badalter")

	// Renames are not supported.
	m, err = NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "t1",
		Alter:       "RENAME COLUMN name TO name2, ADD INDEX(name)", // need both, otherwise INSTANT algorithm will do the rename
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(context.Background())
	assert.Error(t, err) // alter is invalid
	assert.ErrorContains(t, err, "renames are not supported")

	// This is a different type of rename,
	// which is coming via a change
	m, err = NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "t1",
		Alter:       "CHANGE name name2 VARCHAR(255), ADD INDEX(name)", // need both, otherwise INSTANT algorithm will do the rename
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(context.Background())
	assert.Error(t, err) // alter is invalid
	assert.ErrorContains(t, err, "renames are not supported")

	// But this is supported (no rename)
	m, err = NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "t1",
		Alter:       "CHANGE name name VARCHAR(200), ADD INDEX(name)", //nolint: dupword
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(context.Background())
	assert.NoError(t, err) // its valid, no rename
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

	m, err := NewMigrationRunner(&Migration{
		Host:                  TestHost,
		Username:              TestUser,
		Password:              TestPassword,
		Database:              TestSchema,
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

	m, err := NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "lossychange3",
		Alter:       "CHANGE COLUMN b b varchar(200) NOT NULL", //nolint: dupword
		Checksum:    false,
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.NoError(t, err) // works because there are no violations.
	assert.Equal(t, int64(1), m.copier.CopyChunksCount)
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
	m, err := NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "lossychange4",
		Alter:       "CHANGE COLUMN b b varchar(255) NOT NULL", //nolint: dupword
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.Error(t, err) // there is a violation where row 1 is NULL
}

// TestAddUniqueIndex is a really interesting test *because* resuming from checkpoint
// will cause duplicate key errors. It's not straight-forward to differentiate between
// duplicate errors from a resume, and a constraint violation. So what we do is
// 1) *FORCE* checksum to be enabled on resume from checkpoint
// 2) If checksum is not enabled, duplicate key errors are elevated to errors.
func TestAddUniqueIndexChecksumEnabled(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS mytable`)
	table := `CREATE TABLE mytable (
				id int(11) NOT NULL AUTO_INCREMENT,
				name varchar(255) NOT NULL,
				b varchar(255) NOT NULL,
				PRIMARY KEY (id)
			)`
	runSQL(t, table)
	runSQL(t, "INSERT INTO mytable (name, b) VALUES ('a', REPEAT('a', 200))")
	runSQL(t, "INSERT INTO mytable (name, b) VALUES ('a', REPEAT('b', 200))")
	runSQL(t, "INSERT INTO mytable (name, b) VALUES ('a', REPEAT('c', 200))")
	runSQL(t, "INSERT INTO mytable (name, b) VALUES ('a', REPEAT('a', 200))") // duplicate

	m, err := NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "mytable",
		Alter:       "ADD UNIQUE INDEX b (b)",
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.Error(t, err) // not unique

	runSQL(t, "DELETE FROM mytable WHERE b = REPEAT('a', 200) LIMIT 1") // make unique
	m, err = NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "mytable",
		Alter:       "ADD UNIQUE INDEX b (b)",
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.NoError(t, err) // works fine.
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
	m, err := NewMigrationRunner(&Migration{
		Host:        TestHost,
		Username:    TestUser,
		Password:    TestPassword,
		Database:    TestSchema,
		Concurrency: 16,
		Table:       "nonintpk",
		Alter:       "CHANGE COLUMN b b VARCHAR(255) NOT NULL", //nolint: dupword
	})
	assert.NoError(t, err)
	err = m.Run(context.Background())
	assert.NoError(t, err)
}

func TestETA(t *testing.T) {
	t.Skip("skip for now")
	runSQL(t, `DROP TABLE IF EXISTS t1, _t1_shadow`)
	table := `CREATE TABLE t1 (
		uuid varchar(40) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (uuid)
	)`
	runSQL(t, table)

	m, err := NewMigrationRunner(&Migration{
		Host:              TestHost,
		Username:          TestUser,
		Password:          TestPassword,
		Database:          TestSchema,
		Concurrency:       16,
		Table:             "t1",
		Alter:             "ADD INDEX(b)",
		AttemptInplaceDDL: true,
	})
	assert.NoError(t, err)
	logger := log.New(log.LoggingConfig{})
	m.copier, err = copier.NewCopier(nil, nil, nil, 4, true, logger)
	assert.NoError(t, err)

	assert.Equal(t, "Due", m.getETAFromRowsPerSecond(true))
	assert.Equal(t, "-", m.getETAFromRowsPerSecond(false)) // not enough info

	m.copier.EtaRowsPerSecond = 1000
	m.table.EstimatedRows = 1000000

	m.setCurrentState(migrationStateCopyRows)

	assert.Equal(t, "16m40s", m.getETAFromRowsPerSecond(false))
	m.copier.CopyRowsCount = 10000
	assert.Equal(t, "16m30s", m.getETAFromRowsPerSecond(false))
	assert.Equal(t, "copyRows", m.getCurrentState().String())
}

func TestCheckpoint(t *testing.T) {
	tables := []string{`CREATE TABLE t1 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL default 0)`,
	}
	for _, tbl := range tables {
		runSQL(t, `DROP TABLE IF EXISTS t1, _t1_shadow, _t1_cp`)
		runSQL(t, tbl)
		runSQL(t, `insert into t1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM dual`)
		runSQL(t, `insert into t1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM t1`)
		runSQL(t, `insert into t1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM t1 a JOIN t1 b JOIN t1 c`)
		runSQL(t, `insert into t1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM t1 a JOIN t1 b JOIN t1 c`)
		runSQL(t, `insert into t1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM t1 a JOIN t1 LIMIT 100000`) // ~100k rows

		preSetup := func() *MigrationRunner {
			m, err := NewMigrationRunner(&Migration{
				Host:        TestHost,
				Username:    TestUser,
				Password:    TestPassword,
				Database:    TestSchema,
				Concurrency: 16,
				Table:       "t1",
				Alter:       "ENGINE=InnoDB",
			})
			assert.NoError(t, err)
			assert.Equal(t, "initial", m.getCurrentState().String())
			// Usually we would call m.Run() but we want to step through
			// the migration process manually.
			m.db, err = sql.Open("mysql", m.dsn())
			assert.NoError(t, err)
			// Get Table Info
			m.table = table.NewTableInfo(m.schemaName, m.tableName)
			err = m.table.RunDiscovery(m.db)
			assert.NoError(t, err)
			// Attach the correct chunker.
			err = m.table.AttachChunker(m.optTargetChunkMs, m.optDisableTrivialChunker, m.logger)
			assert.NoError(t, err)
			assert.NoError(t, m.dropOldTable())
			return m
		}

		m := preSetup()
		// migrationRunner.Run usually calls m.Setup() here.
		// Which first checks if the table can be restored from checkpoint.
		// Because this is the first run, it can't.

		assert.Error(t, m.resumeFromCheckpoint())

		// So we proceed with the initial steps.
		assert.NoError(t, m.createShadowTable())
		assert.NoError(t, m.alterShadowTable())
		assert.NoError(t, m.createCheckpointTable())
		assert.NoError(t, m.table.Chunker.Open())
		logger := log.New(log.LoggingConfig{})
		m.feed = repl.NewClient(m.db, m.host, m.table, m.shadowTable, m.username, m.password, logger)
		var err error
		m.copier, err = copier.NewCopier(m.db, m.table, m.shadowTable, m.optConcurrency, m.optChecksum, logger)
		assert.NoError(t, err)
		err = m.feed.Run()
		assert.NoError(t, err)

		// Now we are ready to start copying rows.
		// Instead of calling m.copyRows() we will step through it manually.
		// Since we want to checkpoint after a few chunks.

		m.copier.CopyRowsStartTime = time.Now()
		m.setCurrentState(migrationStateCopyRows)
		assert.Equal(t, "copyRows", m.getCurrentState().String())

		// first chunk.
		chunk1, err := m.table.Chunker.Next()
		assert.NoError(t, err)

		chunk2, err := m.table.Chunker.Next()
		assert.NoError(t, err)

		chunk3, err := m.table.Chunker.Next()
		assert.NoError(t, err)

		// There is no watermark yet.
		_, err = m.table.Chunker.GetLowWatermark()
		assert.Error(t, err)
		// Dump checkpoint also returns an error for the same reason.
		assert.Error(t, m.dumpCheckpoint())

		// Because it's multi-threaded, we can't guarantee the order of the chunks.
		assert.NoError(t, m.copier.MigrateChunk(context.TODO(), chunk2))
		assert.NoError(t, m.copier.MigrateChunk(context.TODO(), chunk1))
		assert.NoError(t, m.copier.MigrateChunk(context.TODO(), chunk3))

		// The watermark should exist now, because migrateChunk()
		// gives feedback back to table.

		watermark, err := m.table.Chunker.GetLowWatermark()
		assert.NoError(t, err)
		assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\":1001,\"Inclusive\":true},\"UpperBound\":{\"Value\":2001,\"Inclusive\":false}}", watermark)
		// Dump a checkpoint
		assert.NoError(t, m.dumpCheckpoint())

		// Close the db connection since m is to be destroyed.
		assert.NoError(t, m.db.Close())

		// Now lets imagine that everything fails and we need to start
		// from checkpoint again.

		m = preSetup()
		assert.NoError(t, m.resumeFromCheckpoint())

		// Start the binary log feed just before copy rows starts.
		err = m.feed.Run()
		assert.NoError(t, err)

		// This opens the table at the checkpoint (table.OpenAtWatermark())
		// which sets the chunkPtr at the LowerBound. It also has to position
		// the watermark to this point so new watermarks "align" correctly.
		// So lets now call NextChunk to verify.

		chunk, err := m.table.Chunker.Next()
		assert.NoError(t, err)
		assert.Equal(t, int64(1001), chunk.LowerBound.Value)
		assert.NoError(t, m.copier.MigrateChunk(context.TODO(), chunk))

		// It's ideally not typical but you can still dump checkpoint from
		// a restored checkpoint state. We won't have advanced anywhere from
		// the last checkpoint because on restore, the LowerBound is taken.

		watermark, err = m.table.Chunker.GetLowWatermark()
		assert.NoError(t, err)
		assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\":1001,\"Inclusive\":true},\"UpperBound\":{\"Value\":2001,\"Inclusive\":false}}", watermark)
		// Dump a checkpoint
		assert.NoError(t, m.dumpCheckpoint())

		// Let's confirm we do advance the watermark.
		for i := 0; i < 10; i++ {
			chunk, err = m.table.Chunker.Next()
			assert.NoError(t, err)
			assert.NoError(t, m.copier.MigrateChunk(context.TODO(), chunk))
		}

		watermark, err = m.table.Chunker.GetLowWatermark()
		assert.NoError(t, err)
		assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\":11001,\"Inclusive\":true},\"UpperBound\":{\"Value\":12001,\"Inclusive\":false}}", watermark)
		assert.NoError(t, m.db.Close())
	}
}

// TestE2EBinlogSubscribing is a complex test that uses the lower level interface
// to step through the table while subscribing to changes that we will
// be making to the table between chunks. It is effectively an
// end-to-end test with concurrent operations on the table.
func TestE2EBinlogSubscribing(t *testing.T) {
	// Need to test both composite and non composite keys.
	// Possibly more like mem comparable varbinary.
	tables := []string{`CREATE TABLE t1 (
	id1 INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	id2 INT NOT NULL,
	pad int NOT NULL default 0)`,
		`CREATE TABLE t1 (
		id1 int NOT NULL,
		id2 int not null,
		pad int NOT NULL  default 0,
		PRIMARY KEY (id1, id2))`,
	}

	for _, tbl := range tables {
		runSQL(t, `DROP TABLE IF EXISTS t1, _t1_shadow`)
		runSQL(t, tbl)
		runSQL(t, `insert into t1 (id1, id2) values (1, 1)`)
		runSQL(t, `insert into t1 (id1, id2) values (2, 1)`)
		runSQL(t, `insert into t1 (id1, id2) values (3, 1)`)

		m, err := NewMigrationRunner(&Migration{
			Host:                  TestHost,
			Username:              TestUser,
			Password:              TestPassword,
			Database:              TestSchema,
			Concurrency:           16,
			Table:                 "t1",
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
		m.table = table.NewTableInfo(m.schemaName, m.tableName)
		err = m.table.RunDiscovery(m.db)
		assert.NoError(t, err)
		// Attach the correct chunker.
		err = m.table.AttachChunker(m.optTargetChunkMs, m.optDisableTrivialChunker, m.logger)
		assert.NoError(t, err)
		assert.NoError(t, m.dropOldTable())

		// migration.Run usually calls m.Migrate() here.
		// Which does the following before calling copyRows:
		// So we proceed with the initial steps.
		assert.NoError(t, m.createShadowTable())
		assert.NoError(t, m.alterShadowTable())
		assert.NoError(t, m.createCheckpointTable())
		assert.NoError(t, m.table.Chunker.Open())
		logger := log.New(log.LoggingConfig{})
		m.feed = repl.NewClient(m.db, m.host, m.table, m.shadowTable, m.username, m.password, logger)
		m.copier, err = copier.NewCopier(m.db, m.table, m.shadowTable, m.optConcurrency, m.optChecksum, logger)
		assert.NoError(t, err)
		err = m.feed.Run()
		assert.NoError(t, err)

		// Now we are ready to start copying rows.
		// Instead of calling m.copyRows() we will step through it manually.
		// Since we want to checkpoint after a few chunks.

		m.copier.CopyRowsStartTime = time.Now()
		m.setCurrentState(migrationStateCopyRows)
		assert.Equal(t, "copyRows", m.getCurrentState().String())

		// We expect 3 chunks to be copied.
		// The special first and last case and middle case.

		// first chunk.
		chunk, err := m.table.Chunker.Next()
		assert.NoError(t, err)
		assert.NotNil(t, chunk)
		assert.NoError(t, m.copier.MigrateChunk(context.TODO(), chunk))

		// Now insert some data.
		// This will be ignored by the binlog subscription.
		// Because it's ahead of the high watermark.
		runSQL(t, `insert into t1 (id1, id2) values (4, 1)`)
		assert.True(t, m.table.Chunker.KeyAboveHighWatermark(4))

		// Give it a chance, since we need to read from the binary log to populate this
		// Even though we expect nothing.
		sleep() // plenty
		assert.Equal(t, 0, m.feed.GetDeltaLen())

		// second chunk.
		chunk, err = m.table.Chunker.Next()
		assert.NoError(t, err)
		assert.NoError(t, m.copier.MigrateChunk(context.TODO(), chunk))

		// Now insert some data.
		// This should be picked up by the binlog subscription.
		runSQL(t, `insert into t1 (id1, id2) values (5, 1)`)
		assert.False(t, m.table.Chunker.KeyAboveHighWatermark(5))
		sleep() // wait for binlog
		assert.Equal(t, 1, m.feed.GetDeltaLen())

		runSQL(t, `delete from t1 where id1 = 1`)
		assert.False(t, m.table.Chunker.KeyAboveHighWatermark(1))
		sleep() // wait for binlog
		assert.Equal(t, 2, m.feed.GetDeltaLen())

		// third (and last) chunk.
		chunk, err = m.table.Chunker.Next()
		assert.NoError(t, err)
		assert.NoError(t, m.copier.MigrateChunk(context.TODO(), chunk))

		// Some data is inserted later, even though the last chunk is done.
		// We still care to pick it up.
		runSQL(t, `insert into t1 (id1, id2) values (6, 1)`)
		// the pointer should be at maxint64 for safety. this ensures
		// that any keyAboveHighWatermark checks return false
		assert.False(t, m.table.Chunker.KeyAboveHighWatermark(uint64(math.MaxUint64)))

		// Now that copy rows is done, we flush the changeset until trivial.
		// and perform the optional checksum.
		assert.NoError(t, m.feed.FlushUntilTrivial(context.TODO()))
		m.setCurrentState(migrationStateApplyChangeset)
		assert.Equal(t, "applyChangeset", m.getCurrentState().String())
		m.setCurrentState(migrationStateChecksum)
		assert.NoError(t, m.checksum(context.TODO()))
		assert.Equal(t, "checksum", m.getCurrentState().String())
		// All done!
	}
}

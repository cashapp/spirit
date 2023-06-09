package table

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestChunkerBasic(t *testing.T) {
	t1 := &TableInfo{
		minValue:            newDatum(1, signedType),
		maxValue:            newDatum(1000000, signedType),
		EstimatedRows:       1000000, // avoid trivial chunker.
		SchemaName:          "test",
		TableName:           "t1",
		QuotedName:          "`test`.`t1`",
		PrimaryKey:          []string{"id"},
		pkMySQLTp:           []string{"int"},
		pkDatumTp:           []datumTp{signedType},
		PrimaryKeyIsAutoInc: true,
		Columns:             []string{"id", "name"},
	}
	t1.statisticsLastUpdated = time.Now()
	chunker := &chunkerOptimistic{
		Ti:            t1,
		ChunkerTarget: ChunkerDefaultTarget,
		logger:        logrus.New(),
	}
	chunker.setDynamicChunking(false)

	assert.NoError(t, t1.isCompatibleWithChunker())
	t1.pkMySQLTp[0] = "varchar"
	assert.Error(t, t1.isCompatibleWithChunker())
	t1.pkMySQLTp[0] = "bigint"
	assert.NoError(t, t1.isCompatibleWithChunker())

	assert.Equal(t, "`test`.`t1`", t1.QuotedName)

	assert.NoError(t, chunker.Open())
	_, err := chunker.Next()
	assert.NoError(t, err)

	assert.True(t, chunker.KeyAboveHighWatermark(100)) // we are at 1

	_, err = chunker.Next()
	assert.NoError(t, err)

	assert.False(t, chunker.KeyAboveHighWatermark(100)) // we are at 1001

	for i := 0; i <= 998; i++ {
		_, err = chunker.Next()
		assert.NoError(t, err)
	}

	// The last chunk.
	_, err = chunker.Next()
	assert.NoError(t, err)

	_, err = chunker.Next()
	assert.Error(t, err) // err: table is read.
	assert.Equal(t, err.Error(), "table is read")

	assert.NoError(t, chunker.Close())
}

func TestOpenOnUnsupportedType(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1.EstimatedRows = 1000000 // avoid trivial chunker.
	t1.PrimaryKey = []string{"id"}
	t1.pkMySQLTp = []string{"varchar"}
	t1.PrimaryKeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}

	_, err := NewChunker(t1, 100, logrus.New())
	assert.Error(t, err) // err unsupported.
}

func TestOpenOnBinaryType(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1.EstimatedRows = 1000000 // avoid trivial chunker.
	t1.PrimaryKey = []string{"id"}
	t1.pkMySQLTp = []string{"varbinary"}
	t1.pkDatumTp = []datumTp{binaryType}
	t1.PrimaryKeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := NewChunker(t1, ChunkerDefaultTarget, logrus.New())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())
}

func TestOpenOnNoMinMax(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1.EstimatedRows = 1000000 // avoid trivial chunker.
	t1.PrimaryKey = []string{"id"}
	t1.pkMySQLTp = []string{"varbinary"}
	t1.pkDatumTp = []datumTp{binaryType}
	t1.PrimaryKeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := NewChunker(t1, 100, logrus.New())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())
}

func TestCallingNextChunkWithoutOpen(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1.EstimatedRows = 1000000 // avoid trivial chunker.
	t1.PrimaryKey = []string{"id"}
	t1.pkMySQLTp = []string{"varbinary"}
	t1.pkDatumTp = []datumTp{binaryType}
	t1.PrimaryKeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := NewChunker(t1, 100, logrus.New())
	assert.NoError(t, err)

	_, err = chunker.Next()
	assert.Error(t, err)

	assert.NoError(t, chunker.Open())
	_, err = chunker.Next()
	assert.NoError(t, err)
}

func TestLowWatermark(t *testing.T) {
	t1 := newTableInfo4Test("test", "t1")
	t1.minValue = newDatum(1, signedType)
	t1.maxValue = newDatum(1000000, signedType)
	t1.EstimatedRows = 1000000 // avoid trivial chunker.
	t1.PrimaryKey = []string{"id"}
	t1.pkMySQLTp = []string{"bigint"}
	t1.pkDatumTp = []datumTp{signedType}
	t1.PrimaryKeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}

	assert.NoError(t, t1.isCompatibleWithChunker())
	chunker := &chunkerOptimistic{
		Ti:            t1,
		ChunkerTarget: ChunkerDefaultTarget,
		logger:        logrus.New(),
	}
	chunker.setDynamicChunking(false)

	assert.NoError(t, chunker.Open())

	_, err := chunker.GetLowWatermark()
	assert.Error(t, err)

	chunk, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "id < 1", chunk.String()) // first chunk
	_, err = chunker.GetLowWatermark()
	assert.Error(t, err) // no feedback yet.
	chunker.Feedback(chunk, time.Second)
	_, err = chunker.GetLowWatermark()
	assert.Error(t, err) // there has been feedback, but watermark is not ready after first chunk.

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "id >= 1 AND id < 1001", chunk.String()) // first chunk
	chunker.Feedback(chunk, time.Second)
	watermark, err := chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\": \"1\",\"Inclusive\":true},\"UpperBound\":{\"Value\": \"1001\",\"Inclusive\":false}}", watermark)

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "id >= 1001 AND id < 2001", chunk.String()) // first chunk
	chunker.Feedback(chunk, time.Second)
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\": \"1001\",\"Inclusive\":true},\"UpperBound\":{\"Value\": \"2001\",\"Inclusive\":false}}", watermark)

	chunkAsync1, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "id >= 2001 AND id < 3001", chunkAsync1.String())

	chunkAsync2, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "id >= 3001 AND id < 4001", chunkAsync2.String())

	chunkAsync3, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "id >= 4001 AND id < 5001", chunkAsync3.String())

	chunker.Feedback(chunkAsync2, time.Second)
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\": \"1001\",\"Inclusive\":true},\"UpperBound\":{\"Value\": \"2001\",\"Inclusive\":false}}", watermark)

	chunker.Feedback(chunkAsync3, time.Second)
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\": \"1001\",\"Inclusive\":true},\"UpperBound\":{\"Value\": \"2001\",\"Inclusive\":false}}", watermark)

	chunker.Feedback(chunkAsync1, time.Second)
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\": \"4001\",\"Inclusive\":true},\"UpperBound\":{\"Value\": \"5001\",\"Inclusive\":false}}", watermark)

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "id >= 5001 AND id < 6001", chunk.String()) // should bump immediately
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\": \"4001\",\"Inclusive\":true},\"UpperBound\":{\"Value\": \"5001\",\"Inclusive\":false}}", watermark)

	chunker.Feedback(chunk, time.Second)
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\": \"5001\",\"Inclusive\":true},\"UpperBound\":{\"Value\": \"6001\",\"Inclusive\":false}}", watermark)
}

func TestDynamicChunking(t *testing.T) {
	t1 := newTableInfo4Test("test", "t1")
	t1.minValue = newDatum(1, signedType)
	t1.maxValue = newDatum(1000000, signedType)
	t1.EstimatedRows = 1000000
	t1.PrimaryKey = []string{"id"}
	t1.pkMySQLTp = []string{"bigint"}
	t1.pkDatumTp = []datumTp{signedType}
	t1.PrimaryKeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := NewChunker(t1, 100*time.Millisecond, logrus.New())
	assert.NoError(t, err)

	assert.NoError(t, chunker.Open())

	chunk, err := chunker.Next()
	assert.NoError(t, err)
	chunker.Feedback(chunk, time.Second) // way too long.

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), chunk.ChunkSize) // immediate change from before
	chunker.Feedback(chunk, time.Second)          // way too long again, it will reduce to 10

	newChunk, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), newChunk.ChunkSize) // immediate change from before
	// Feedback is only taken if the chunk.ChunkSize matches the current size.
	// so lets give bad feedback and see no change.
	newChunk.ChunkSize = 1234
	chunker.Feedback(newChunk, 10*time.Second) // way too long.

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), chunk.ChunkSize) // no change
	chunker.Feedback(chunk, 50*time.Microsecond) //must give feedback to advance watermark.

	// Feedback to increase the chunk size is more gradual.
	for i := 0; i < 10; i++ { // no change
		chunk, err = chunker.Next()
		chunker.Feedback(chunk, 50*time.Microsecond) // very short.
		assert.NoError(t, err)
		assert.Equal(t, uint64(10), chunk.ChunkSize) // no change.
	}
	// On the 11th piece of feedback *with this chunk size*
	// it finally changes. But no greater than 50% increase at a time.
	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, uint64(15), chunk.ChunkSize)
	chunker.Feedback(chunk, 50*time.Microsecond)

	// Advance the watermark a little bit.
	for i := 0; i < 20; i++ {
		chunk, err = chunker.Next()
		assert.NoError(t, err)
		chunker.Feedback(chunk, time.Millisecond)
	}

	// Fetch the watermark.
	watermark, err := chunker.GetLowWatermark()
	assert.NoError(t, err)

	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":22,\"LowerBound\":{\"Value\": \"584\",\"Inclusive\":true},\"UpperBound\":{\"Value\": \"606\",\"Inclusive\":false}}", watermark)

	// Start everything over again as t2.
	t2 := newTableInfo4Test("test", "t1")
	t2.minValue = newDatum(1, signedType)
	t2.maxValue = newDatum(1000000, signedType)
	t2.EstimatedRows = 1000000
	t2.PrimaryKey = []string{"id"}
	t2.pkMySQLTp = []string{"bigint"}
	t2.PrimaryKeyIsAutoInc = true

	chunker2, err := NewChunker(t1, 100, logrus.New())
	assert.NoError(t, err)
	t2.Columns = []string{"id", "name"}
	assert.NoError(t, chunker2.OpenAtWatermark(watermark))

	// The pointer goes to the lowerbound.value.
	// It could equally go to the upperbound.value but then
	// we would have to worry about off-by-1 errors.
	chunk, err = chunker2.Next()
	assert.NoError(t, err)
	assert.Equal(t, "584", chunk.LowerBound.Value.String())
}

func dsn() string {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return "msandbox:msandbox@tcp(127.0.0.1:8030)/test"
	}
	return dsn
}

func newTableInfo4Test(schema, table string) *TableInfo { //nolint: unparam
	t1 := NewTableInfo(nil, schema, table)
	return t1
}

func runSQL(t *testing.T, stmt string) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(stmt)
	assert.NoError(t, err)
}

func TestDiscovery(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS t1`)
	table := `CREATE TABLE t1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	runSQL(t, `insert into t1 values (1, 'a'), (2, 'b'), (3, 'c')`)

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "t1")
	assert.NoError(t, t1.SetInfo(context.TODO()))

	assert.Equal(t, "t1", t1.TableName)
	assert.Equal(t, "test", t1.SchemaName)
	assert.Equal(t, "id", t1.PrimaryKey[0])

	// normalize for mysql 5.7 and 8.0
	assert.Equal(t, "int", removeWidth(t1.colTps["id"]))
	assert.Equal(t, "CAST(`id` AS signed)", t1.WrapCastType("id"))
	assert.Equal(t, "CAST(`name` AS char)", t1.WrapCastType("name"))

	assert.Equal(t, "1", t1.minValue.String())
	assert.Equal(t, "3", t1.maxValue.String())

	//runSQL(t, `insert into t1 values (4, 'a'), (5, 'b'), (6, 'c')`)
	//assert.NoError(t, t1.UpdateTableStatistics(db))
	//assert.Equal(t, int64(1), t1.minValue)
	//assert.Equal(t, int64(6), t1.maxValue)

	// Can't check estimated rows (depends on MySQL version etc)
	assert.Equal(t, []string{"int"}, t1.pkMySQLTp)
	assert.True(t, t1.PrimaryKeyIsAutoInc)
	assert.Equal(t, 2, len(t1.Columns))
}

func TestDiscoveryUInt(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS t1`)
	table := `CREATE TABLE t1 (
		id int(11) unsigned NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, table)
	runSQL(t, `insert into t1 values (1, 'a'), (2, 'b'), (3, 'c')`)

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "t1")
	assert.NoError(t, t1.SetInfo(context.TODO()))

	assert.Equal(t, "t1", t1.TableName)
	assert.Equal(t, "test", t1.SchemaName)
	assert.Equal(t, "id", t1.PrimaryKey[0])

	assert.Equal(t, "1", t1.minValue.String())
	assert.Equal(t, "3", t1.maxValue.String())

	//runSQL(t, `insert into t1 values (4, 'a'), (5, 'b'), (6, 'c')`)
	//assert.NoError(t, t1.UpdateTableStatistics(db))
	//assert.Equal(t, uint64(1), t1.minValue)
	//assert.Equal(t, uint64(6), t1.maxValue)

	// Can't check estimated rows (depends on MySQL version etc)
	assert.Equal(t, []string{"int unsigned"}, t1.pkMySQLTp)
	assert.True(t, t1.PrimaryKeyIsAutoInc)
	assert.Equal(t, 2, len(t1.Columns))
}

func TestDiscoveryNoPrimaryKeyOrNoTable(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS t1`)
	table := `CREATE TABLE t1 (
		id int(11) NOT NULL,
		name varchar(255) NOT NULL
	)`
	runSQL(t, table)
	runSQL(t, `insert into t1 values (1, 'a'), (2, 'b'), (3, 'c')`)

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "t1")
	assert.Error(t, t1.SetInfo(context.TODO()))

	t2 := NewTableInfo(db, "test", "t2fdsfds")
	assert.ErrorContains(t, t2.SetInfo(context.TODO()), "table test.t2fdsfds does not exist")
}

func TestDiscoveryBalancesTable(t *testing.T) {
	// This is not a bad test, since there is a PRIMARY KEY and a UNIQUE KEY
	// and the discovery has to discover the primary key as the constraint
	// not the unique key.
	table := `CREATE TABLE balances (
		id bigint NOT NULL AUTO_INCREMENT,
		b_token varbinary(255) NOT NULL,
		c_token varbinary(255) NOT NULL,
		version int NOT NULL DEFAULT '0',
		cents bigint NOT NULL,
		currency varbinary(3) NOT NULL,
		c1 varchar(50) NOT NULL,
		c2 varchar(120) DEFAULT NULL,
		b1 tinyint NOT NULL,
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (id),
		UNIQUE KEY b_token (b_token),
		KEY c_token (c_token)
	  ) ENGINE=InnoDB AUTO_INCREMENT=3000001 DEFAULT CHARSET=utf8mb4`
	runSQL(t, `drop table if exists balances`)
	runSQL(t, table)

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "balances")
	assert.NoError(t, t1.SetInfo(context.TODO()))

	assert.True(t, t1.PrimaryKeyIsAutoInc)
	assert.Equal(t, []string{"bigint"}, t1.pkMySQLTp)
	assert.Equal(t, []string{"id"}, t1.PrimaryKey)
	assert.Equal(t, "0", t1.minValue.String())
	assert.Equal(t, "0", t1.maxValue.String())

	chunker, err := NewChunker(t1, 100, logrus.New())
	assert.NoError(t, err)

	assert.NoError(t, chunker.Open())
	assert.Equal(t, "0", t1.minValue.String())
	assert.Equal(t, "0", t1.maxValue.String())
}

func TestDiscoveryCompositeNonComparable(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS compnoncomparable`)
	table := `CREATE TABLE compnoncomparable (
		id int(11) unsigned NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id, name)
	)`
	runSQL(t, table)
	runSQL(t, `insert into compnoncomparable values (1, 'a'), (2, 'b'), (3, 'c')`)

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "compnoncomparable")
	assert.Error(t, t1.SetInfo(context.TODO()))
}

func TestDiscoveryCompositeComparable(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS compcomparable`)
	table := `CREATE TABLE compcomparable (
		id int(11) unsigned NOT NULL AUTO_INCREMENT,
		age int(11) NOT NULL,
		PRIMARY KEY (id, age)
	)`
	runSQL(t, table)
	runSQL(t, `insert into compcomparable values (1, 1), (2, 2), (3, 3)`)

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "compcomparable")
	assert.NoError(t, t1.SetInfo(context.TODO()))

	assert.True(t, t1.PrimaryKeyIsAutoInc)
	assert.Equal(t, []string{"int unsigned", "int"}, t1.pkMySQLTp)
	assert.Equal(t, []string{"id", "age"}, t1.PrimaryKey)
}

func TestStatisticsUpdate(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)

	runSQL(t, `DROP TABLE IF EXISTS statsupdate`)
	table := `CREATE TABLE statsupdate (
		id int(11) unsigned NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id, name)
	)`
	runSQL(t, table)
	runSQL(t, `insert into statsupdate values (1, 'a'), (2, 'b'), (3, 'c')`)

	t1 := &TableInfo{
		minValue:            newDatum(1, signedType),
		maxValue:            newDatum(1000000, signedType),
		EstimatedRows:       1000000, // avoid trivial chunker.
		SchemaName:          "test",
		TableName:           "statsupdate",
		QuotedName:          "`test`.`statsupdate`",
		PrimaryKey:          []string{"id"},
		pkMySQLTp:           []string{"int"},
		pkDatumTp:           []datumTp{signedType},
		PrimaryKeyIsAutoInc: true,
		Columns:             []string{"id", "name"},
		db:                  db,
	}
	t1.statisticsLastUpdated = time.Now()

	go t1.AutoUpdateStatistics(context.Background(), time.Millisecond*10, logrus.New())
	time.Sleep(time.Millisecond * 100)

	t1.Close()
}

func TestPrimaryKeyValuesExtraction(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	runSQL(t, `DROP TABLE IF EXISTS t1`)
	table := `CREATE TABLE t1 (
		id int(11) unsigned NOT NULL AUTO_INCREMENT,
		name varchar(115),
		age int(11) NOT NULL,
		PRIMARY KEY (id, age)
	)`
	runSQL(t, table)
	runSQL(t, `insert into t1 values (1, 'a', 15), (2, 'b', 20), (3, 'c', 25)`)

	t1 := NewTableInfo(db, "test", "t1")
	assert.NoError(t, t1.SetInfo(context.TODO()))

	var id, age int
	var name string

	err = db.QueryRow("SELECT * FROM `test`.`t1` ORDER BY id DESC LIMIT 1").Scan(&id, &name, &age)
	assert.NoError(t, err)

	row := []interface{}{id, name, age}
	pkVals := t1.PrimaryKeyValues(row)
	assert.Equal(t, id, pkVals[0])
	assert.Equal(t, age, pkVals[1])
}

func TestPrefetchChunking(t *testing.T) {
	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	runSQL(t, `DROP TABLE IF EXISTS tprefetch`)
	table := `CREATE TABLE tprefetch (
		id BIGINT NOT NULL AUTO_INCREMENT,
		created_at DATETIME(3) NULL,
		PRIMARY KEY (id)
	)`
	runSQL(t, table)

	// insert about 11K rows.
	runSQL(t, `INSERT INTO tprefetch (created_at) VALUES (NULL)`)
	runSQL(t, `INSERT INTO tprefetch (created_at) SELECT NULL FROM tprefetch a JOIN tprefetch b JOIN tprefetch c`)
	runSQL(t, `INSERT INTO tprefetch (created_at) SELECT NULL FROM tprefetch a JOIN tprefetch b JOIN tprefetch c`)
	runSQL(t, `INSERT INTO tprefetch (created_at) SELECT NULL FROM tprefetch a JOIN tprefetch b JOIN tprefetch c`)
	runSQL(t, `INSERT INTO tprefetch (created_at) SELECT NULL FROM tprefetch a JOIN tprefetch b LIMIT 10000`)

	// the max id should be able 11040
	// lets insert one far off ID: 300B
	// and then continue inserting at greater than the max dynamic chunk size.
	runSQL(t, `INSERT INTO tprefetch (id, created_at) VALUES (300000000000, NULL)`)
	runSQL(t, `INSERT INTO tprefetch (created_at) SELECT NULL FROM tprefetch a JOIN tprefetch b LIMIT 300000`)

	// and then another big gap
	// and then continue inserting at greater than the max dynamic chunk size.
	runSQL(t, `INSERT INTO tprefetch (id, created_at) VALUES (600000000000, NULL)`)
	runSQL(t, `INSERT INTO tprefetch (created_at) SELECT NULL FROM tprefetch a JOIN tprefetch b LIMIT 300000`)
	// and then one final value which is way out there.
	runSQL(t, `INSERT INTO tprefetch (id, created_at) VALUES (900000000000, NULL)`)

	t1 := newTableInfo4Test("test", "tprefetch")
	t1.db = db
	assert.NoError(t, t1.SetInfo(context.Background()))
	chunker := &chunkerOptimistic{
		Ti:            t1,
		ChunkerTarget: time.Second,
		logger:        logrus.New(),
	}
	chunker.setDynamicChunking(true)
	assert.NoError(t, chunker.Open())
	assert.False(t, chunker.chunkPrefetchingEnabled)

	for !chunker.finalChunkSent {
		chunk, err := chunker.Next()
		assert.NoError(t, err)
		chunker.Feedback(chunk, 100*time.Millisecond) // way too short.
	}
	assert.True(t, chunker.chunkPrefetchingEnabled)
}

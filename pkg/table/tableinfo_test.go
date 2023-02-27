package table

import (
	"context"
	"database/sql"
	"math"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestChunkerBasic(t *testing.T) {
	t1 := &TableInfo{
		minValue:            1,
		maxValue:            1000000,
		EstimatedRows:       1000000, // avoid trivial chunker.
		SchemaName:          "test",
		TableName:           "t1",
		PrimaryKey:          []string{"id"},
		primaryKeyType:      "int",
		primaryKeyIsAutoInc: true,
		Columns:             []string{"id", "name"},
	}
	chunker, err := NewChunker(t1, 100, false, logrus.New())
	assert.NoError(t, err)
	chunker.SetDynamicChunking(false)

	assert.NoError(t, t1.isCompatibleWithChunker())
	t1.primaryKeyType = "varchar"
	assert.Error(t, t1.isCompatibleWithChunker())
	t1.primaryKeyType = "bigint"
	assert.NoError(t, t1.isCompatibleWithChunker())

	assert.Equal(t, "`test`.`t1`", t1.QuotedName())

	assert.NoError(t, chunker.Open())
	_, err = chunker.Next()
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
}

/*
func TestChunkerStatic(t *testing.T) {

	t1 := NewTableInfo("test", "t1")
	t1.minValue = int64(1)
	t1.maxValue = int64(1000000)
	t1.EstimatedRows = 1000000 // avoid trivial chunker.
	t1.PrimaryKey = []string{"id"}
	t1.primaryKeyType = "bigint"
	t1.primaryKeyIsAutoInc = true

	t1.Columns = []string{"id", "name"}

	assert.NoError(t, t1.isCompatibleWithChunker())
	assert.NoError(t, t1.attachChunker())
	chunker.SetDynamicChunking(false)

	assert.NoError(t, chunker.Open())

	chunk, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "id < 1", chunk.String()) // first chunk

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "id >= 1 AND id < 1001", chunk.String()) // first chunk

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "id >= 1001 AND id < 2001", chunk.String()) // nth chunk

	t1.chunkSize = 10000
	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "id >= 2001 AND id < 12001", chunk.String()) // nth chunk

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "id >= 12001 AND id < 22001", chunk.String()) // nth chunk

	t1.chunkSize = 10000000
	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "id >= 22001 AND id < 10022001", chunk.String()) // nth chunk

	// last chunk
	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "id >= 10022001", chunk.String())

}
*/

func TestOpenOnUnsupportedType(t *testing.T) {
	t1 := NewTableInfo("test", "t1")
	t1.minValue = int64(1)
	t1.maxValue = int64(1000000)
	t1.EstimatedRows = 1000000 // avoid trivial chunker.
	t1.PrimaryKey = []string{"id"}
	t1.primaryKeyType = "varchar(100)"
	t1.primaryKeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}

	_, err := NewChunker(t1, 100, true, logrus.New())
	assert.Error(t, err) // err unsupported.
}

func TestOpenOnBinaryType(t *testing.T) {
	t1 := NewTableInfo("test", "t1")
	t1.minValue = int64(1)
	t1.maxValue = int64(1000000)
	t1.EstimatedRows = 1000000 // avoid trivial chunker.
	t1.PrimaryKey = []string{"id"}
	t1.primaryKeyType = "varbinary(100)"
	t1.primaryKeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := NewChunker(t1, 100, true, logrus.New())
	assert.NoError(t, err)
	chunker.SetDynamicChunking(false)
	assert.NoError(t, chunker.Open())
}

func TestOpenOnNoMinMax(t *testing.T) {
	t1 := NewTableInfo("test", "t1")
	t1.EstimatedRows = 1000000 // avoid trivial chunker.
	t1.PrimaryKey = []string{"id"}
	t1.primaryKeyType = "varbinary(100)"
	t1.primaryKeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := NewChunker(t1, 100, true, logrus.New())
	assert.NoError(t, err)
	chunker.SetDynamicChunking(false)
	assert.NoError(t, chunker.Open())
}

func TestCallingNextChunkWithoutOpen(t *testing.T) {
	t1 := NewTableInfo("test", "t1")
	t1.EstimatedRows = 1000000 // avoid trivial chunker.
	t1.PrimaryKey = []string{"id"}
	t1.primaryKeyType = "varbinary(100)"
	t1.primaryKeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := NewChunker(t1, 100, true, logrus.New())
	assert.NoError(t, err)
	chunker.SetDynamicChunking(false)

	_, err = chunker.Next()
	assert.Error(t, err)

	assert.NoError(t, chunker.Open())
	_, err = chunker.Next()
	assert.NoError(t, err)
}

/*
func TestChunkToIncrementSize(t *testing.T) {

	t1 := NewTableInfo("test", "t1")
	t1.EstimatedRows = 1000000 // avoid trivial chunker.
	t1.PrimaryKey = []string{"id"}
	t1.primaryKeyType = "bigint unsigned"
	t1.Columns = []string{"id", "name"}
	assert.NoError(t, t1.attachChunker())
	chunker.SetDynamicChunking(false)

	assert.NoError(t, chunker.Open())

	// With no min or max, the estimated rows is divided
	// by a wide range.
	assert.Equal(t, uint64(18446744073709552), t1.chunkToIncrementSize())

	// With min and max, the rows per chunk is smaller.
	// It should be 1 to 1 because the estimated rows is 1000000
	t1.minValue = uint64(1)
	t1.maxValue = uint64(1000000)
	assert.Equal(t, uint64(1000), t1.chunkToIncrementSize())

	// If the maxValue is doubled, the effective chunk-to-increment increases.
	t1.maxValue = uint64(2000000)
	assert.Equal(t, uint64(2000), t1.chunkToIncrementSize())

	// However, we disable this optimization for auto-inc primary keys.
	// Because there's a likelihood there could be holes in the sequence,
	// and stumbling upon a large chunk without holes could be a problem.
	// Maybe in future we will stop this specific behavior and treat all equal,
	// TBD.
	t1.primaryKeyIsAutoInc = true
	assert.Equal(t, uint64(1000), t1.chunkToIncrementSize())

	// Check for signed int values, should behave similar.
	t1.primaryKeyType = "bigint"
	t1.minValue = int64(1)
	t1.maxValue = int64(2000000)
	assert.Equal(t, uint64(1000), t1.chunkToIncrementSize())
	t1.primaryKeyIsAutoInc = false
	assert.Equal(t, uint64(2000), t1.chunkToIncrementSize())

}
*/

func TestExtractPrimaryKeyFromRowImage(t *testing.T) {

	// TODO

}

func TestLowWatermark(t *testing.T) {
	t1 := newTableInfo4Test("test", "t1")
	t1.minValue = int64(1)
	t1.maxValue = int64(1000000)
	t1.EstimatedRows = 1000000 // avoid trivial chunker.
	t1.PrimaryKey = []string{"id"}
	t1.primaryKeyType = "bigint"
	t1.primaryKeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}

	assert.NoError(t, t1.isCompatibleWithChunker())
	chunker, err := NewChunker(t1, 100, true, logrus.New())
	assert.NoError(t, err)
	chunker.SetDynamicChunking(false)

	assert.NoError(t, chunker.Open())

	_, err = chunker.GetLowWatermark()
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
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\":1,\"Inclusive\":true},\"UpperBound\":{\"Value\":1001,\"Inclusive\":false}}", watermark)

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "id >= 1001 AND id < 2001", chunk.String()) // first chunk
	chunker.Feedback(chunk, time.Second)
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\":1001,\"Inclusive\":true},\"UpperBound\":{\"Value\":2001,\"Inclusive\":false}}", watermark)

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
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\":1001,\"Inclusive\":true},\"UpperBound\":{\"Value\":2001,\"Inclusive\":false}}", watermark)

	chunker.Feedback(chunkAsync3, time.Second)
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\":1001,\"Inclusive\":true},\"UpperBound\":{\"Value\":2001,\"Inclusive\":false}}", watermark)

	chunker.Feedback(chunkAsync1, time.Second)
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\":4001,\"Inclusive\":true},\"UpperBound\":{\"Value\":5001,\"Inclusive\":false}}", watermark)

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "id >= 5001 AND id < 6001", chunk.String()) // should bump immediately
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\":4001,\"Inclusive\":true},\"UpperBound\":{\"Value\":5001,\"Inclusive\":false}}", watermark)

	chunker.Feedback(chunk, time.Second)
	watermark, err = chunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":1000,\"LowerBound\":{\"Value\":5001,\"Inclusive\":true},\"UpperBound\":{\"Value\":6001,\"Inclusive\":false}}", watermark)
}

func TestDynamicChunking(t *testing.T) {
	t1 := newTableInfo4Test("test", "t1")
	t1.minValue = int64(1)
	t1.maxValue = int64(1000000)
	t1.EstimatedRows = 1000000
	t1.PrimaryKey = []string{"id"}
	t1.primaryKeyType = "bigint"
	t1.primaryKeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := NewChunker(t1, 100, true, logrus.New())
	assert.NoError(t, err)
	chunker.SetDynamicChunking(true)

	assert.NoError(t, chunker.Open())

	chunk, err := chunker.Next()
	assert.NoError(t, err)
	chunker.Feedback(chunk, time.Second) // way too long.

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), chunk.ChunkSize) // immediate change from before
	chunker.Feedback(chunk, time.Second)          // way too long again, it will reduce to 20

	newChunk, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, uint64(20), newChunk.ChunkSize) // immediate change from before
	// Feedback is only taken if the chunk.ChunkSize matches the current size.
	// so lets give bad feedback and see no change.
	newChunk.ChunkSize = 1234
	chunker.Feedback(newChunk, 10*time.Second) // way too long.

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, uint64(20), chunk.ChunkSize) // no change
	chunker.Feedback(chunk, 50*time.Microsecond) //must give feedback to advance watermark.

	// Feedback to increase the chunk size is more gradual.
	for i := 0; i < 10; i++ { // no change
		chunk, err = chunker.Next()
		chunker.Feedback(chunk, 50*time.Microsecond) // very short.
		assert.NoError(t, err)
		assert.Equal(t, uint64(20), chunk.ChunkSize) // no change.
	}
	// On the 11st piece of feedback *with this chunk size*
	// it finally changes. But no greater than 50% increase at a time.
	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, uint64(30), chunk.ChunkSize)
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

	assert.Equal(t, "{\"Key\":\"id\",\"ChunkSize\":45,\"LowerBound\":{\"Value\":1076,\"Inclusive\":true},\"UpperBound\":{\"Value\":1121,\"Inclusive\":false}}", watermark)

	// Start everything over again as t2.
	t2 := newTableInfo4Test("test", "t1")
	t2.minValue = int64(1)
	t2.maxValue = int64(1000000)
	t2.EstimatedRows = 1000000
	t2.PrimaryKey = []string{"id"}
	t2.primaryKeyType = "bigint"
	t2.primaryKeyIsAutoInc = true

	chunker2, err := NewChunker(t1, 100, true, logrus.New())
	assert.NoError(t, err)
	chunker2.SetDynamicChunking(true)
	t2.Columns = []string{"id", "name"}
	assert.NoError(t, chunker2.OpenAtWatermark(watermark))

	// The pointer goes to the lowerbound.value.
	// It could equally go to the upperbound.value but then
	// we would have to worry about off-by-1 errors.
	chunk, err = chunker2.Next()
	assert.NoError(t, err)
	assert.Equal(t, int64(1076), chunk.LowerBound.Value)
}

// These tests require a DB connection.

func dsn() string {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return "msandbox:msandbox@tcp(127.0.0.1:8030)/test"
	}
	return dsn
}

func newTableInfo4Test(schema, table string) *TableInfo {
	t1 := NewTableInfo(schema, table)
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

	t1 := NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(context.TODO(), db))

	assert.Equal(t, "t1", t1.TableName)
	assert.Equal(t, "test", t1.SchemaName)
	assert.Equal(t, "id", t1.PrimaryKey[0])

	assert.Equal(t, int64(1), t1.minValue)
	assert.Equal(t, int64(3), t1.maxValue)

	//runSQL(t, `insert into t1 values (4, 'a'), (5, 'b'), (6, 'c')`)
	//assert.NoError(t, t1.UpdateTableStatistics(db))
	//assert.Equal(t, int64(1), t1.minValue)
	//assert.Equal(t, int64(6), t1.maxValue)

	// Can't check estimated rows (depends on MySQL version etc)
	assert.Equal(t, "int", t1.primaryKeyType)
	assert.True(t, t1.primaryKeyIsAutoInc)
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

	t1 := NewTableInfo("test", "t1")
	assert.NoError(t, t1.RunDiscovery(context.TODO(), db))

	assert.Equal(t, "t1", t1.TableName)
	assert.Equal(t, "test", t1.SchemaName)
	assert.Equal(t, "id", t1.PrimaryKey[0])

	assert.Equal(t, uint64(1), t1.minValue)
	assert.Equal(t, uint64(3), t1.maxValue)

	//runSQL(t, `insert into t1 values (4, 'a'), (5, 'b'), (6, 'c')`)
	//assert.NoError(t, t1.UpdateTableStatistics(db))
	//assert.Equal(t, uint64(1), t1.minValue)
	//assert.Equal(t, uint64(6), t1.maxValue)

	// Can't check estimated rows (depends on MySQL version etc)
	assert.Equal(t, "int unsigned", t1.primaryKeyType)
	assert.True(t, t1.primaryKeyIsAutoInc)
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

	t1 := NewTableInfo("test", "t1")
	assert.Error(t, t1.RunDiscovery(context.TODO(), db))

	t2 := NewTableInfo("test", "t2fdsfds")
	assert.Error(t, t2.RunDiscovery(context.TODO(), db))
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

	t1 := NewTableInfo("test", "balances")
	assert.NoError(t, t1.RunDiscovery(context.TODO(), db))

	assert.True(t, t1.primaryKeyIsAutoInc)
	assert.Equal(t, "bigint", t1.primaryKeyType)
	assert.Equal(t, []string{"id"}, t1.PrimaryKey)
	assert.Equal(t, nil, t1.minValue)
	assert.Equal(t, nil, t1.maxValue)

	chunker, err := NewChunker(t1, 100, false, logrus.New())
	assert.NoError(t, err)

	assert.NoError(t, chunker.Open())
	assert.Equal(t, int64(math.MinInt64), t1.minValue)
	assert.Equal(t, int64(math.MaxInt64), t1.maxValue)
}

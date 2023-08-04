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

func TestOpenOnUnsupportedType(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1.EstimatedRows = 1000000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"varchar"}
	t1.keyDatums = []datumTp{unknownType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}

	_, err := NewChunker(t1, 100, logrus.New())
	assert.Error(t, err) // err unsupported.

	// Also unsupported as part of a composite key.
	// When the first part of the key is supported.
	t1.KeyColumns = []string{"id", "otherkey"}
	t1.keyColumnsMySQLTp = []string{"bigint", "varchar"}
	t1.keyDatums = []datumTp{signedType, unknownType}
	_, err = NewChunker(t1, 100, logrus.New())
	assert.Error(t, err) // err unsupported.
}

func TestOpenOnBinaryType(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1.EstimatedRows = 1000000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"varbinary"}
	t1.keyDatums = []datumTp{binaryType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := NewChunker(t1, ChunkerDefaultTarget, logrus.New())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())
}

func TestOpenOnNoMinMax(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1.EstimatedRows = 1000000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"varbinary"}
	t1.keyDatums = []datumTp{binaryType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := NewChunker(t1, 100, logrus.New())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())
}

func TestCallingNextChunkWithoutOpen(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1.EstimatedRows = 1000000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"varbinary"}
	t1.keyDatums = []datumTp{binaryType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := NewChunker(t1, 100, logrus.New())
	assert.NoError(t, err)

	_, err = chunker.Next()
	assert.Error(t, err)

	assert.NoError(t, chunker.Open())
	_, err = chunker.Next()
	assert.NoError(t, err)
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
	assert.Equal(t, "id", t1.KeyColumns[0])

	// normalize for mysql 5.7 and 8.0
	assert.Equal(t, "int", removeWidth(t1.columnsMySQLTps["id"]))
	assert.Equal(t, "CAST(`id` AS signed)", t1.WrapCastType("id"))
	assert.Equal(t, "CAST(`name` AS char)", t1.WrapCastType("name"))

	assert.Equal(t, "1", t1.minValue.String())
	assert.Equal(t, "3", t1.maxValue.String())

	//runSQL(t, `insert into t1 values (4, 'a'), (5, 'b'), (6, 'c')`)
	//assert.NoError(t, t1.UpdateTableStatistics(db))
	//assert.Equal(t, int64(1), t1.minValue)
	//assert.Equal(t, int64(6), t1.maxValue)

	// Can't check estimated rows (depends on MySQL version etc)
	assert.Equal(t, []string{"int"}, t1.keyColumnsMySQLTp)
	assert.True(t, t1.KeyIsAutoInc)
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
	assert.Equal(t, "id", t1.KeyColumns[0])

	assert.Equal(t, "1", t1.minValue.String())
	assert.Equal(t, "3", t1.maxValue.String())

	//runSQL(t, `insert into t1 values (4, 'a'), (5, 'b'), (6, 'c')`)
	//assert.NoError(t, t1.UpdateTableStatistics(db))
	//assert.Equal(t, uint64(1), t1.minValue)
	//assert.Equal(t, uint64(6), t1.maxValue)

	// Can't check estimated rows (depends on MySQL version etc)
	assert.Equal(t, []string{"int unsigned"}, t1.keyColumnsMySQLTp)
	assert.True(t, t1.KeyIsAutoInc)
	assert.Equal(t, 2, len(t1.Columns))
}

func TestDiscoveryNoKeyColumnsOrNoTable(t *testing.T) {
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

	assert.True(t, t1.KeyIsAutoInc)
	assert.Equal(t, []string{"bigint"}, t1.keyColumnsMySQLTp)
	assert.Equal(t, []string{"id"}, t1.KeyColumns)
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

	assert.True(t, t1.KeyIsAutoInc)
	assert.Equal(t, []string{"int unsigned", "int"}, t1.keyColumnsMySQLTp)
	assert.Equal(t, []string{"id", "age"}, t1.KeyColumns)
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
		minValue:          newDatum(1, signedType),
		maxValue:          newDatum(1000000, signedType),
		EstimatedRows:     1000000,
		SchemaName:        "test",
		TableName:         "statsupdate",
		QuotedName:        "`test`.`statsupdate`",
		KeyColumns:        []string{"id"},
		keyColumnsMySQLTp: []string{"int"},
		keyDatums:         []datumTp{signedType},
		KeyIsAutoInc:      true,
		Columns:           []string{"id", "name"},
		db:                db,
	}
	t1.statisticsLastUpdated = time.Now()

	go t1.AutoUpdateStatistics(context.Background(), time.Millisecond*10, logrus.New())
	time.Sleep(time.Millisecond * 100)

	t1.Close()
}

func TestKeyColumnsValuesExtraction(t *testing.T) {
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

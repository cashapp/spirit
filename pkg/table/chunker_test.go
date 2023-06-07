package table

import (
	"context"
	"database/sql"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestCompositeChunker(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS composite`)
	table := `CREATE TABLE composite (
		id bigint NOT NULL AUTO_INCREMENT,
		age int(11) NOT NULL,
		PRIMARY KEY (id, age)
	)`
	runSQL(t, table)

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "composite")
	assert.NoError(t, t1.SetInfo(context.TODO()))

	chunker, _ := NewChunker(t1, 0, logrus.New())
	assert.IsType(t, &chunkerComposite{}, chunker)
}

func TestUniversalChunker(t *testing.T) {
	runSQL(t, `DROP TABLE IF EXISTS universal`)
	table := `CREATE TABLE universal (
		id bigint NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	)`
	runSQL(t, table)

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "universal")
	assert.NoError(t, t1.SetInfo(context.TODO()))

	chunker, _ := NewChunker(t1, 0, logrus.New())
	assert.IsType(t, &chunkerUniversal{}, chunker)
}

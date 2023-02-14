package table

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestBaseChunker(t *testing.T) {
	t1 := NewTableInfo("test", "t1")
	t1.EstimatedRows = 100 // target trivial chunker.
	t1.PrimaryKey = []string{"id"}
	t1.primaryKeyType = "varbinary(100)"
	t1.primaryKeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}

	chunker, err := NewChunker(t1, 100, false, logrus.New())
	assert.NoError(t, err)

	assert.NoError(t, chunker.Open())
	_, err = chunker.Next()
	assert.NoError(t, err)

	_, err = chunker.Next()
	assert.Error(t, err) // trivial chunker is done! only 1 chunk.
}

func TestBaseChunkerIntSigned(t *testing.T) {
	t1 := NewTableInfo("test", "t1")
	t1.EstimatedRows = 100 // target trivial chunker.
	t1.PrimaryKey = []string{"id"}
	t1.primaryKeyType = "bigint"
	t1.primaryKeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := NewChunker(t1, 100, false, logrus.New())
	assert.NoError(t, err)

	assert.NoError(t, chunker.Open())
	_, err = chunker.Next()
	assert.NoError(t, err)

	_, err = chunker.Next()
	assert.Error(t, err) // trivial chunker is done! only 1 chunk.
}

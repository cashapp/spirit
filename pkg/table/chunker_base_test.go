package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBaseChunker(t *testing.T) {
	t1 := NewTableInfo("test", "t1")
	t1.EstimatedRows = 100 // target trivial chunker.
	t1.PrimaryKey = []string{"id"}
	t1.primaryKeyType = "varbinary(100)"
	t1.primaryKeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	assert.NoError(t, t1.AttachChunker(100, false, nil))

	assert.NoError(t, t1.Chunker.Open())
	_, err := t1.Chunker.Next()
	assert.NoError(t, err)

	_, err = t1.Chunker.Next()
	assert.Error(t, err) // trivial chunker is done! only 1 chunk.
}

func TestBaseChunkerIntSigned(t *testing.T) {
	t1 := NewTableInfo("test", "t1")
	t1.EstimatedRows = 100 // target trivial chunker.
	t1.PrimaryKey = []string{"id"}
	t1.primaryKeyType = "bigint"
	t1.primaryKeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	assert.NoError(t, t1.AttachChunker(100, false, nil))

	assert.NoError(t, t1.Chunker.Open())
	_, err := t1.Chunker.Next()
	assert.NoError(t, err)

	_, err = t1.Chunker.Next()
	assert.Error(t, err) // trivial chunker is done! only 1 chunk.
}

package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnsignedChunker(t *testing.T) {
	t1 := NewTableInfo("test", "t1")
	t1.EstimatedRows = 100
	t1.PrimaryKey = []string{"id"}
	t1.primaryKeyType = "bigint unsigned"
	t1.primaryKeyIsAutoInc = true
	t1.EstimatedRows = 100000
	t1.minValue = uint64(1)
	t1.maxValue = uint64(101)
	t1.Columns = []string{"id", "name"}
	assert.NoError(t, t1.AttachChunker(100, true, nil))

	assert.NoError(t, t1.Chunker.Open())

	_, err := t1.Chunker.Next() // min
	assert.NoError(t, err)

	_, err = t1.Chunker.Next() // 1 row in between
	assert.NoError(t, err)

	_, err = t1.Chunker.Next() // max
	assert.NoError(t, err)

	_, err = t1.Chunker.Next()
	assert.Error(t, err) // is read
}

package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBinaryChunker(t *testing.T) {
	t1 := NewTableInfo("test", "t1")
	t1.EstimatedRows = 100000
	t1.PrimaryKey = []string{"id"}
	t1.primaryKeyType = "varbinary(100)"
	//t1.DisableTrivialChunker = true
	t1.minValue = uint64(1)
	t1.maxValue = uint64(101)
	t1.Columns = []string{"id", "name"}
	assert.NoError(t, t1.AttachChunker(100, true, nil)) // attaches binary chunker.

	assert.NoError(t, t1.Chunker.Open())

	// min and max are not used in binary chunker.
	// so we need to access the whole keyspace.
	// Because we set the estimated rows to low, that should be quick.

	var i int64
	for ; !t1.Chunker.IsRead(); i++ {
		_, err := t1.Chunker.Next()
		if err != nil {
			break
		}
	}
	_, err := t1.Chunker.Next() // still read
	assert.Error(t, err)
	assert.Less(t, i, int64(100)) // 100000 rows est should be less than 100 chunks
}

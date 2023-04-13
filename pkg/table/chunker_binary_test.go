package table

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestBinaryChunker(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1.statisticsLastUpdated = time.Now()
	t1.EstimatedRows = 100000
	t1.PrimaryKey = []string{"id"}
	t1.primaryKeyType = "varbinary(100)"
	t1.minValue = uint64(1)
	t1.maxValue = uint64(101)
	t1.Columns = []string{"id", "name"}
	chunker, err := NewChunker(t1, 100, true, logrus.New())
	assert.NoError(t, err)

	assert.NoError(t, chunker.Open())

	// min and max are not used in binary chunker.
	// so we need to access the whole keyspace.
	// Because we set the estimated rows to low, that should be quick.

	var i int64
	for ; !chunker.IsRead(); i++ {
		_, err := chunker.Next()
		if err != nil {
			break
		}
	}
	_, err = chunker.Next() // still read
	assert.Error(t, err)
	assert.Less(t, i, int64(100)) // 100000 rows est should be less than 100 chunks
}

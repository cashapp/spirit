package table

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestUnsignedChunker(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1.statisticsLastUpdated = time.Now()
	t1.EstimatedRows = 100
	t1.PrimaryKey = []string{"id"}
	t1.primaryKeyType = "bigint unsigned"
	t1.PrimaryKeyIsAutoInc = true
	t1.EstimatedRows = 100000
	t1.minValue = uint64(1)
	t1.maxValue = uint64(101)
	t1.Columns = []string{"id", "name"}

	chunker, err := NewChunker(t1, 100, true, logrus.New())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())

	_, err = chunker.Next() // min
	assert.NoError(t, err)

	_, err = chunker.Next() // 1 row in between
	assert.NoError(t, err)

	_, err = chunker.Next() // max
	assert.NoError(t, err)

	_, err = chunker.Next()
	assert.Error(t, err) // is read
}

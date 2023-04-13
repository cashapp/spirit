package table

import (
	"context"
	"math"
)

type chunkerSigned struct {
	*chunkerBase
}

var _ Chunker = &chunkerSigned{}

func (t *chunkerSigned) Next() (*Chunk, error) {
	t.Lock()
	defer t.Unlock()
	if t.IsRead() {
		return nil, ErrTableIsRead
	}
	if !t.isOpen {
		return nil, ErrTableNotOpen
	}
	// We only range off the first part of the key for simplicity.
	key := t.Ti.PrimaryKey[0]
	incrementSize := int64(t.chunkToIncrementSize()) // adjust for composite keys and large gaps in sequence

	// The minimum value is pre-read from the table. If it returned
	// NULL, Open() populates it with the minimum int64 value.

	if t.chunkPtr == nil {
		t.chunkPtr = t.Ti.minValue.(int64)
		return &Chunk{
			ChunkSize:  t.chunkSize,
			Key:        key,
			UpperBound: &Boundary{t.chunkPtr, false},
		}, nil
	}

	// Before we return a final open bounded chunk, we check if the statistics
	// need updating, in which case we synchronously refresh them.
	// This helps reduce the risk of a very large unbounded
	// chunk from a table that is actively growing.
	if t.chunkPtr.(int64) >= t.Ti.maxValue.(int64) && t.Ti.statisticsNeedUpdating() {
		t.logger.Info("approaching the end of the table, synchronously updating statistics")
		if err := t.Ti.updateTableStatistics(context.TODO()); err != nil {
			return nil, err
		}
	}

	// *Only now* if the chunkPtr equals or exceeds the maximum value,
	// we return a final open bounded chunk.
	if t.chunkPtr.(int64) >= t.Ti.maxValue.(int64) {
		minVal := t.chunkPtr
		t.chunkPtr = int64(math.MaxInt64)
		t.finalChunkSent = true
		return &Chunk{
			ChunkSize:  t.chunkSize,
			Key:        key,
			LowerBound: &Boundary{minVal, true},
		}, nil
	}

	// This is the typical case. We return a chunk with a lower bound
	// of the current chunkPtr and an upper bound of the chunkPtr + chunkSize,
	// but not exceeding math.MaxInt64.
	minVal := t.chunkPtr.(int64)
	maxVal := t.chunkPtr.(int64) + incrementSize
	if maxVal < incrementSize {
		maxVal = math.MaxInt64 // Overflow has occurred
	}

	t.chunkPtr = maxVal
	return &Chunk{
		ChunkSize:  t.chunkSize,
		Key:        key,
		LowerBound: &Boundary{minVal, true},
		UpperBound: &Boundary{maxVal, false},
	}, nil
}

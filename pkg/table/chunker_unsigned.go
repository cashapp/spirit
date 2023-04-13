package table

import (
	"context"
	"math"
)

type chunkerUnsigned struct {
	*chunkerBase
}

var _ Chunker = &chunkerUnsigned{}

func (t *chunkerUnsigned) Next() (*Chunk, error) {
	t.Lock()
	defer t.Unlock()
	if t.IsRead() {
		return nil, ErrTableIsRead
	}
	if !t.isOpen {
		return nil, ErrTableNotOpen
	}
	key := t.Ti.PrimaryKey[0]
	incrementSize := t.chunkToIncrementSize() // a helper function better estimate this.

	// If there is a minimum value, we attempt to apply
	// the minimum value optimization.
	if t.chunkPtr == nil {
		t.chunkPtr = t.Ti.minValue.(uint64)
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
	if t.chunkPtr.(uint64) >= t.Ti.maxValue.(uint64) && t.Ti.statisticsNeedUpdating() {
		t.logger.Info("approaching the end of the table, synchronously updating statistics")
		if err := t.Ti.updateTableStatistics(context.TODO()); err != nil {
			return nil, err
		}
	}

	// Only now if there is a maximum value and the chunkPtr exceeds it, we apply
	// the maximum value optimization which is to return an open bounded
	// chunk.
	if t.Ti.maxValue != nil && t.chunkPtr.(uint64) > t.Ti.maxValue.(uint64) {
		minVal := t.chunkPtr
		t.chunkPtr = uint64(math.MaxUint64)
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

	minVal := t.chunkPtr.(uint64)
	maxVal := t.chunkPtr.(uint64) + incrementSize
	if maxVal < incrementSize {
		maxVal = math.MaxUint64 // Overflow has occurred
	}

	t.chunkPtr = maxVal
	return &Chunk{
		ChunkSize:  t.chunkSize,
		Key:        key,
		LowerBound: &Boundary{minVal, true},
		UpperBound: &Boundary{maxVal, false},
	}, nil
}

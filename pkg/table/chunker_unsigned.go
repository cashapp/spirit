package table

import "math"

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

	// If there is a maximum value and the chunkPtr exceeds it, we apply
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

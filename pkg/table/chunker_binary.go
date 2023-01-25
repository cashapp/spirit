package table

import "math"

type chunkerBinary struct {
	*chunkerBase
}

var _ Chunker = &chunkerBinary{}

// nextChunkBinary is called under a mutex, we know its been read.
// We only range off the first part of the key for simplicity.
// For binary keys we store the chunkPtr as an int64.
// The minValue/maxValue optimization has to convert the string to an int64 (hard)
// The chunker also has to return strings.
func (t *chunkerBinary) Next() (*Chunk, error) {
	t.Lock()
	defer t.Unlock()
	if t.IsRead() {
		return nil, ErrTableIsRead
	}
	if !t.isOpen {
		return nil, ErrTableNotOpen
	}

	key := t.Ti.PrimaryKey[0]
	incrementSize := t.chunkToIncrementSize() // adjust for composite keys and large gaps in sequence

	// We don't (yet) apply min value optimization on binary strings.
	// Instead we rely on chunkToIncrementSize() being useful.
	// And if the chunkPtr is nil, we initialize off 0.

	// First chunk
	if t.chunkPtr == nil {
		t.chunkPtr = uint64(0)
		return &Chunk{
			ChunkSize:  t.chunkSize,
			Key:        key,
			UpperBound: &Boundary{hexString(t.chunkPtr.(uint64)), false},
		}, nil
	}
	// Final chunk
	if t.chunkPtr.(uint64) >= math.MaxInt64 {
		minVal := hexString(t.chunkPtr.(uint64))
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
		LowerBound: &Boundary{hexString(minVal), true},
		UpperBound: &Boundary{hexString(maxVal), false},
	}, nil
}

package table

import (
	"errors"
	"sync"
	"time"

	"github.com/siddontang/loggers"
)

// This is the trivial chunker which other chunkers may extend
type chunkerTrivial struct {
	sync.Mutex
	Ti             *TableInfo
	isOpen         bool
	finalChunkSent bool

	// Some options which are usually good, but might want to be disabled
	// particularly for the test suite.
	DisableDynamicChunker bool // optimization to adjust chunk size.

	logger loggers.Advanced
}

var _ Chunker = &chunkerTrivial{}

// Open opens a table to be used by NextChunk(). See also OpenAtWatermark()
// to resume from a specific point.
func (t *chunkerTrivial) Open() (err error) {
	t.isOpen = true
	return nil
}

func (t *chunkerTrivial) SetDynamicChunking(newValue bool) {
}

func (t *chunkerTrivial) OpenAtWatermark(cp string) error {
	return nil
}

func (t *chunkerTrivial) Close() error {
	return nil
}

func (t *chunkerTrivial) Next() (*Chunk, error) {
	t.Lock()
	defer t.Unlock()
	if t.IsRead() {
		return nil, ErrTableIsRead
	}
	if !t.isOpen {
		return nil, ErrTableNotOpen
	}
	t.finalChunkSent = true
	return &Chunk{
		ChunkSize: 1000,
		Key:       t.Ti.PrimaryKey[0],
	}, nil
}

// ChunkerFeedback is a way for consumers of chunks to give feedback on how long
// processing the chunk took. It is incorporated into the calculation of future
// chunk sizes.
func (t *chunkerTrivial) Feedback(chunk *Chunk, d time.Duration) {
}

// GetLowWatermark returns the highest known value that has been safely copied,
// which (due to parallelism) could be significantly behind the high watermark.
// The value is discovered via ChunkerFeedback(), and when retrieved from this func
// can be used to write a checkpoint for restoration.
func (t *chunkerTrivial) GetLowWatermark() (string, error) {
	t.Lock()
	defer t.Unlock()
	return "", errors.New("watermark not yet ready")
}

func (t *chunkerTrivial) IsRead() bool {
	return t.finalChunkSent
}

func (t *chunkerTrivial) KeyAboveHighWatermark(key interface{}) bool {
	t.Lock()
	defer t.Unlock()
	return true
}

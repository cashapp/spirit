package table

import (
	"time"

	"github.com/siddontang/loggers"
)

var (
	// StartingChunkSize is the initial chunkSize
	// it is a const, but set to var for tests.
	StartingChunkSize uint64 = 1000
)

const (
	// MaxDynamicStepFactor is the maximum amount each recalculation of the dynamic chunkSize can
	// increase by. For example, if the newTarget is 5000 but the current target is 1000, the newTarget
	// will be capped back down to 1500. Over time the number 5000 will be reached, but not straight away.
	MaxDynamicStepFactor = 1.5
	// MinDynamicRowSize is the minimum chunkSize that can be used when dynamic chunkSize is enabled.
	// This helps prevent a scenario where the chunk size is too small (it can never be less than 1).
	MinDynamicRowSize = 10
	// MaxDynamicRowSize is the max allowed chunkSize that can be used when dynamic chunkSize is enabled.
	// This seems like a safe upper bound for now
	MaxDynamicRowSize = 100000
	// DynamicPanicFactor is the factor by which the feedback process takes immediate action when
	// the chunkSize appears to be too large. For example, if the PanicFactor is 5, and the target *time*
	// is 50ms, an actual time 250ms+ will cause the dynamic chunk size to immediately be reduced.
	DynamicPanicFactor = 5

	// ChunkerDefaultTarget is the default chunker target
	ChunkerDefaultTarget = 100 * time.Millisecond
)

type Chunker interface {
	Open() error
	OpenAtWatermark(string, Datum) error
	IsRead() bool
	Close() error
	Next() (*Chunk, error)
	Feedback(*Chunk, time.Duration)
	GetLowWatermark() (string, error)
	KeyAboveHighWatermark(interface{}) bool
}

func NewChunker(t *TableInfo, chunkerTarget time.Duration, logger loggers.Advanced) (Chunker, error) {
	if chunkerTarget == 0 {
		chunkerTarget = ChunkerDefaultTarget
	}
	// Use the optimistic chunker for auto_increment
	// tables with a single column key.
	if len(t.KeyColumns) == 1 && t.KeyIsAutoInc {
		return &chunkerOptimistic{
			Ti:                     t,
			ChunkerTarget:          chunkerTarget,
			lowerBoundWatermarkMap: make(map[string]*Chunk, 0),
			logger:                 logger,
		}, nil
	}
	return NewCompositeChunker(t, chunkerTarget, logger, "", "")
}

// NewCompositeChunker returns a chunkerComposite ,
// setting its Key if keyName and where conditions are provided
func NewCompositeChunker(t *TableInfo, chunkerTarget time.Duration, logger loggers.Advanced, keyName string, whereCondition string) (Chunker, error) {
	c := chunkerComposite{
		Ti:                     t,
		ChunkerTarget:          chunkerTarget,
		lowerBoundWatermarkMap: make(map[string]*Chunk, 0),
		logger:                 logger,
	}
	var err error
	if keyName != "" && whereCondition != "" {
		err = c.SetKey(keyName, whereCondition)
	}
	return &c, err
}

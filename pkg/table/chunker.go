package table

import (
	"time"

	"github.com/siddontang/loggers"
)

const (
	// StartingChunkSize is the original value of chunkSize.
	// We use it in some threshold calculations.
	StartingChunkSize = 1000

	// MaxDynamicScaleFactor is the maximum factor dynamic scaling can change the chunkSize from
	// the setting chunkSize. For example, if the factor is 10, and chunkSize is 1000, then the
	// values will be in the range of 100 to 10000.
	MaxDynamicScaleFactor = 50
	// MaxDynamicStepFactor is the maximum amount each recalculation of the dynamic chunkSize can
	// increase by. For example, if the newTarget is 5000 but the current target is 1000, the newTarget
	// will be capped back down to 1500. Over time the number 5000 will be reached, but not straight away.
	MaxDynamicStepFactor = 1.5
	// MinDynamicChunkSize is the minimum chunkSize that can be used when dynamic chunkSize is enabled.
	// This helps prevent a scenario where the chunk size is too small (it can never be less than 1).
	MinDynamicRowSize = 10
	// DynamicPanicFactor is the factor by which the feedback process takes immediate action when
	// the chunkSize appears to be too large. For example, if the PanicFactor is 5, and the target *time*
	// is 50ms, an actual time 250ms+ will cause the dynamic chunk size to immediately be reduced.
	DynamicPanicFactor = 5
)

type Chunker interface {
	Open() error
	OpenAtWatermark(string) error
	IsRead() bool
	Close() error
	Next() (*Chunk, error)
	Feedback(*Chunk, time.Duration)
	GetLowWatermark() (string, error)
	KeyAboveHighWatermark(interface{}) bool
	SetDynamicChunking(bool)
}

var _ Chunker = &chunkerBase{}

func NewChunker(t *TableInfo, chunkerTarget time.Duration, disableTrivialChunker bool, logger loggers.Advanced) (Chunker, error) {
	if t.EstimatedRows < trivialChunkerThreshold && !disableTrivialChunker {
		// If the row count is low we attach the trivial chunker,
		// which will return everything as one chunk.
		return &chunkerBase{Ti: t, logger: logger}, nil
	}
	if chunkerTarget == 0 {
		chunkerTarget = 100 * time.Millisecond
	}
	chunkerBase := &chunkerBase{
		Ti:            t,
		chunkSize:     uint64(1000),  // later this might become configurable.
		ChunkerTarget: chunkerTarget, // this is the main chunker target.
		logger:        logger,
	}
	switch mySQLTypeToSimplifiedKeyType(t.primaryKeyType) {
	case signedType:
		return &chunkerSigned{
			chunkerBase: chunkerBase,
		}, nil
	case unsignedType:
		return &chunkerUnsigned{
			chunkerBase: chunkerBase,
		}, nil
	case binaryType:
		return &chunkerBinary{
			chunkerBase: chunkerBase,
		}, nil
	default:
		return nil, ErrUnsupportedPKType
	}
}

package table

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
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

// This is the trivial chunker which other chunkers may extend
type chunkerBase struct {
	sync.Mutex
	Ti             *TableInfo
	chunkSize      uint64
	chunkPtr       interface{}
	finalChunkSent bool
	isOpen         bool

	// Dynamic Chunking is time based instead of row based.
	// It uses *time* to determine the target chunk size.
	chunkTimingInfo []time.Duration
	ChunkerTargetMs int64 // i.e. 100ms for target

	// Some options which are usually good, but might want to be disabled
	// particularly for the test suite.
	DisableDynamicChunker bool // optimization to adjust chunk size.

	// This is used for restore.
	watermark             *Chunk
	watermarkQueuedChunks []*Chunk
}

// Open opens a table to be used by NextChunk(). See also OpenAtWatermark()
// to resume from a specific point.
func (t *chunkerBase) Open() (err error) {
	t.Lock()
	defer t.Unlock()

	return t.open()
}

func (t *chunkerBase) SetDynamicChunking(newValue bool) {
	t.Lock()
	defer t.Unlock()
	t.DisableDynamicChunker = !newValue
}

func (t *chunkerBase) OpenAtWatermark(cp string) error {
	t.Lock()
	defer t.Unlock()

	// Open the table first.
	// This will reset the chunk pointer, but we'll set it before the mutex
	// is released.
	if err := t.open(); err != nil {
		return err
	}

	var chunk Chunk
	err := json.Unmarshal([]byte(cp), &chunk)
	if err != nil {
		return err
	}

	t.watermark = &chunk

	// We can restore from chunk.UpperBound, but because it is a < operator,
	// There might be an annoying off by 1 error. So let's just restore
	// from the chunk.LowerBound.
	tp := mySQLTypeToSimplifiedKeyType(t.Ti.primaryKeyType)
	// The chunk.LowerBound.Value is an interface{} so upon deserialization
	// from JSON it will be a float64. We need to convert it to tp
	t.chunkPtr, err = simplifyType(tp, chunk.LowerBound.Value)
	return err
}

func (t *chunkerBase) Close() error {
	return nil
}

func (t *chunkerBase) Next() (*Chunk, error) {
	t.Lock()
	defer t.Unlock()
	if t.IsRead() {
		return nil, ErrTableIsRead
	}
	if !t.isOpen {
		return nil, ErrTableNotOpen
	}
	switch mySQLTypeToSimplifiedKeyType(t.Ti.primaryKeyType) {
	case signedType:
		t.chunkPtr = int64(math.MaxInt64)
	case unsignedType, binaryType:
		t.chunkPtr = uint64(math.MaxUint64)
	case unknownType:
		return nil, fmt.Errorf("unknown primary key type: %s", t.Ti.primaryKeyType) // should be unreachable because Open() already checks this.
	}
	t.finalChunkSent = true
	return &Chunk{
		ChunkSize: t.chunkSize,
		Key:       t.Ti.PrimaryKey[0],
	}, nil
}

// ChunkerFeedback is a way for consumers of chunks to give feedback on how long
// processing the chunk took. It is incorporated into the calculation of future
// chunk sizes.
func (t *chunkerBase) Feedback(chunk *Chunk, d time.Duration) {
	t.Lock()
	defer t.Unlock()

	t.bumpWatermark(chunk)

	// Check if the feedback is based on an earlier chunker size.
	// if it is, it is misleading to incorporate feedback now.
	// We should just skip it. We also skip if dynamic chunking is disabled.
	if chunk.ChunkSize != t.chunkSize || t.DisableDynamicChunker {
		return
	}

	// If any copyRows tasks take 5x the target size we reduce immediately
	// and don't wait for more feedback.
	if int64(d) > (t.ChunkerTargetMs * DynamicPanicFactor * int64(time.Millisecond)) {
		newTarget := uint64(float64(t.chunkSize) / float64(DynamicPanicFactor*2))

		t.Ti.logger.WithFields(log.Fields{
			"time":            d,
			"threshold":       time.Duration(t.ChunkerTargetMs * DynamicPanicFactor * int64(time.Millisecond)),
			"target-rows":     t.chunkSize,
			"target-ms":       t.ChunkerTargetMs,
			"new-target-rows": newTarget,
		}).Debug("high chunk processing time")

		t.updateChunkerTarget(newTarget)
		return
	}

	// Add feedback to the list.
	t.chunkTimingInfo = append(t.chunkTimingInfo, d)

	// We have enough feedback to re-evaluate the chunk size.
	if len(t.chunkTimingInfo) > 10 {
		t.updateChunkerTarget(t.calculateNewTargetChunkSize())
	}
}

// GetLowWatermark returns the highest known value that has been safely copied,
// which (due to parallelism) could be significantly behind the high watermark.
// The value is discovered via ChunkerFeedback(), and when retrieved from this func
// can be used to write a checkpoint for restoration.
func (t *chunkerBase) GetLowWatermark() (string, error) {
	t.Lock()
	defer t.Unlock()

	if t.watermark == nil || t.watermark.UpperBound == nil || t.watermark.LowerBound == nil {
		return "", fmt.Errorf("watermark not yet ready")
	}

	// Convert the watermark to JSON.
	b, err := json.Marshal(t.watermark)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// isSpecialRestoredChunk is used to test for the first chunk after restore-from-checkpoint.
// The restored chunk is a really special beast because the lowerbound
// will be repeated by the first chunk that is applied post restore.
// But the types might not match because JSON encoding/restoring will use float64.
// This is called under a mutex.
func (t *chunkerBase) isSpecialRestoredChunk(chunk *Chunk) bool {
	if chunk.LowerBound == nil || chunk.UpperBound == nil || t.watermark.LowerBound == nil || t.watermark.UpperBound == nil {
		return false // restored checkpoints always have both.
	}
	tp := mySQLTypeToSimplifiedKeyType(t.Ti.primaryKeyType)
	val1, err1 := simplifyType(tp, chunk.LowerBound.Value)
	val2, err2 := simplifyType(tp, t.watermark.LowerBound.Value)
	if err1 != nil || err2 != nil {
		return false
	}
	return val1 == val2
}

// bumpWatermark updates the minimum value that is known to be safely copied,
// and is called under a mutex.
// Because of parallelism, it is possible that a chunk is copied out of order,
// so this func needs to account for that.
// The current algorithm is N^2 complexity, but hopefully that's not a big deal.
// Basically:
//   - If the chunk does not "align" to the current low watermark, it's added to a queue.
//   - If it does align, the watermark is bumped to the chunk's max value. Then
//     each of the queued chunks is checked to see if it aligns with the new watermark.
//   - If any queued chunks align, they are popped off the queue and the watermark is bumped.
//   - This process repeats until there is no more alignment from the queue *or* the queue is empty.
func (t *chunkerBase) bumpWatermark(chunk *Chunk) {
	// Special handling for the first chunk since we need it to have boundaries
	// for other chunks to merge to.
	if t.watermark == nil {
		if chunk.LowerBound != nil {
			// Not the first chunk, append it and move on!
			t.watermarkQueuedChunks = append(t.watermarkQueuedChunks, chunk)
			return
		}
		// Else, we know this is the first chunk
		t.watermark = chunk
	} else {
		// We already have a watermark, we just need to test if we need to replace it.
		// Or if we should add it to the queue.
		if chunk.LowerBound == nil {
			// This is the first chunk we are seeing (possibly again?) even though we
			// already have a watermark. This is not expected to happen. It could mean
			// that the table has been re-opened (the API now tries to prevent this).
			panic("unexpected first chunk after watermark set")
		}
		if !t.isSpecialRestoredChunk(chunk) && t.watermark.UpperBound.Value != chunk.LowerBound.Value {
			t.watermarkQueuedChunks = append(t.watermarkQueuedChunks, chunk)
			return
		}
		// t.watermark.UpperBound.Value == chunk.LowerBound.Value
		// Replace the current watermark with the chunk.
		t.watermark = chunk
	}

	// If we haven't returned early, it means there could be queued chunks.
	// So we should process them.

	for {
		// Check the queue for any chunks that align with the new watermark.
		// If there are any, pop them off the queue and bump the watermark.
		// If there are none, we're done.
		found := false
		for i, queuedChunk := range t.watermarkQueuedChunks {
			if queuedChunk.LowerBound.Value == t.watermark.UpperBound.Value {
				t.watermark = queuedChunk
				t.watermarkQueuedChunks = append(t.watermarkQueuedChunks[:i], t.watermarkQueuedChunks[i+1:]...)
				found = true
				break
			}
		}
		if !found {
			break
		}
	}
}

func (t *chunkerBase) open() (err error) {
	tp := mySQLTypeToSimplifiedKeyType(t.Ti.primaryKeyType)
	if tp == unknownType {
		return ErrUnsupportedPKType
	}
	if t.isOpen {
		// This prevents an error where open is re-called
		// leading to the watermark being in a strange state.
		return fmt.Errorf("table is already open, did you mean to call Reset()?")
	}
	t.isOpen = true
	t.chunkPtr = nil
	t.finalChunkSent = false
	// Ensure min/max are already simplified so Next() operators
	// can use them with just type assertions.
	if tp == binaryType {
		// We don't apply min/max optimization on binary yet.
		// instead we rely on the chunker expanding the range
		// in chunkToIncrementSize().
		t.Ti.maxValue = uint64(math.MaxUint64)
		t.Ti.minValue = uint64(0)
	} else {
		t.Ti.minValue, err = simplifyType(tp, t.Ti.minValue)
		if err != nil {
			return err
		}
		t.Ti.maxValue, err = simplifyType(tp, t.Ti.maxValue)
		if err != nil {
			return err
		}
	}

	// Make sure min/max value are always specified
	// To simplify the code in NextChunk funcs.
	if t.Ti.minValue == nil {
		switch tp {
		case signedType:
			t.Ti.minValue = int64(math.MinInt64)
		case unsignedType, binaryType:
			t.Ti.minValue = uint64(0)
		default:
			return ErrUnsupportedPKType
		}
	}
	if t.Ti.maxValue == nil {
		switch tp {
		case signedType:
			t.Ti.maxValue = int64(math.MaxInt64)
		case unsignedType, binaryType:
			t.Ti.maxValue = uint64(math.MaxUint64)
		default:
			return ErrUnsupportedPKType
		}
	}
	return nil
}

func (t *chunkerBase) IsRead() bool {
	return t.finalChunkSent
}

func (t *chunkerBase) chunkToIncrementSize() uint64 {
	// already called under a mutex.
	var increment = t.chunkSize

	if t.Ti.primaryKeyIsAutoInc {
		// There is a case when there are large "gaps" in the table because
		// the difference between min<->max is larger than the estimated rows.
		// Estimated rows is often wrong, and it could just be a large chunk deleted,
		// which could get us in trouble with some large chunks. So for now we should
		// just return the chunk size when auto_increment.
		return increment
	}

	// Logical range is MaxValue-MinValue. If it matches close to estimated rows,
	// then the increment will be the same as the chunk size. If it's higher or lower,
	// then the increment will be smaller or larger.
	logicalRange := t.logicalRange()
	divideBy := float64(t.Ti.EstimatedRows) / float64(logicalRange)
	return uint64(math.Ceil(float64(increment) / divideBy)) // ceil to guarantee at least 1 for progress.
}

func (t *chunkerBase) logicalRange() uint64 {
	if mySQLTypeToSimplifiedKeyType(t.Ti.primaryKeyType) == signedType {
		return uint64(t.Ti.maxValue.(int64) - t.Ti.minValue.(int64))
	}
	if mySQLTypeToSimplifiedKeyType(t.Ti.primaryKeyType) == unsignedType {
		return t.Ti.maxValue.(uint64) - t.Ti.minValue.(uint64)
	}
	// For binary types, we can't really do anything useful yet:
	return math.MaxUint64
}

func (t *chunkerBase) updateChunkerTarget(newTarget uint64) {
	// Already called under a mutex.
	newTarget = t.boundaryCheckTargetChunkSize(newTarget)
	t.chunkSize = newTarget
	t.chunkTimingInfo = []time.Duration{}
}

func (t *chunkerBase) boundaryCheckTargetChunkSize(newTarget uint64) uint64 {
	newTargetRows := float64(newTarget)
	referenceSize := float64(StartingChunkSize)
	if newTargetRows < (referenceSize / MaxDynamicScaleFactor) {
		newTargetRows = referenceSize / MaxDynamicScaleFactor
	}
	if newTargetRows > (referenceSize * MaxDynamicScaleFactor) {
		newTargetRows = referenceSize * MaxDynamicScaleFactor
	}
	if newTargetRows > float64(t.chunkSize)*MaxDynamicStepFactor {
		newTargetRows = float64(t.chunkSize) * MaxDynamicStepFactor
	}
	if newTargetRows < MinDynamicRowSize {
		newTargetRows = MinDynamicRowSize
	}
	return uint64(newTargetRows)
}

func (t *chunkerBase) calculateNewTargetChunkSize() uint64 {
	// We do all our math as float64 of time in ns
	p90 := float64(lazyFindP90(t.chunkTimingInfo))
	targetTime := float64(t.ChunkerTargetMs * int64(time.Millisecond))
	newTargetRows := float64(t.chunkSize) * (targetTime / p90)
	return uint64(newTargetRows)
}

func (t *chunkerBase) KeyAboveHighWatermark(key interface{}) bool {
	t.Lock()
	defer t.Unlock()
	if t.chunkPtr == nil {
		return true // every key is above!
	}

	keyType := reflect.TypeOf(key)
	chunkPtrType := reflect.TypeOf(t.chunkPtr)

	if keyType.ConvertibleTo(chunkPtrType) {
		// Convert key to the same type as chunkPtr
		// Although they should be the same already.
		keyValue := reflect.ValueOf(key).Convert(chunkPtrType)
		chunkPtrValue := reflect.ValueOf(t.chunkPtr)

		// Use >= since in the chunker the upper bound is usually non-inclusive.
		switch chunkPtrType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return keyValue.Int() >= chunkPtrValue.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return keyValue.Uint() >= chunkPtrValue.Uint()
		default:
			return false
		}
	}
	return false
}

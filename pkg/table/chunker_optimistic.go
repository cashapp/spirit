package table

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/siddontang/loggers"
)

type chunkerOptimistic struct {
	sync.Mutex
	Ti                *TableInfo
	chunkSize         uint64
	chunkPtr          datum
	checkpointHighPtr datum // the high watermark detected on restore
	finalChunkSent    bool
	isOpen            bool

	// Dynamic Chunking is time based instead of row based.
	// It uses *time* to determine the target chunk size.
	chunkTimingInfo []time.Duration
	ChunkerTarget   time.Duration // i.e. 500ms for target

	disableDynamicChunker bool // only used by the test suite

	// This is used for restore.
	watermark             *Chunk
	watermarkQueuedChunks []*Chunk

	// The chunk prefetching algorithm is used when the chunker detects
	// that there are very large gaps in the sequence.
	chunkPrefetchingEnabled bool

	logger loggers.Advanced
}

var _ Chunker = &chunkerOptimistic{}

// nextChunkByPrefetching uses prefetching instead of feedback to determine the chunk size.
// It is used when the chunker detects that there are very large gaps in the sequence.
// When this mode is enabled, the chunkSize is "reset" to 1000 rows, so we know that
// t.chunkSize is reliable. It is also expanded again based on feedback.
func (t *chunkerOptimistic) nextChunkByPrefetching() (*Chunk, error) {
	key := t.Ti.KeyColumns[0]
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s LIMIT 1 OFFSET %d",
		key, t.Ti.QuotedName, key, key, t.chunkSize,
	)
	rows, err := t.Ti.db.Query(query, t.chunkPtr.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		minVal := t.chunkPtr
		var upperVal int64
		err = rows.Scan(&upperVal)
		if err != nil {
			return nil, err
		}
		maxVal := newDatum(upperVal, t.chunkPtr.tp)
		t.chunkPtr = maxVal

		// If the difference between min and max is less than
		// MaxDynamicRowSize we can turn off prefetching.
		if maxVal.Range(minVal) < MaxDynamicRowSize {
			t.logger.Warnf("disabling chunk prefetching: min-val=%s max-val=%s max-dynamic-row-size=%d", minVal, maxVal, MaxDynamicRowSize)
			t.chunkSize = StartingChunkSize // reset
			t.chunkPrefetchingEnabled = false
		}

		return &Chunk{
			ChunkSize:  t.chunkSize,
			Key:        key,
			LowerBound: &Boundary{minVal, true},
			UpperBound: &Boundary{maxVal, false},
		}, nil
	}
	// If there were no rows, it means we are indeed
	// on the final chunk.
	t.finalChunkSent = true
	return &Chunk{
		ChunkSize:  t.chunkSize,
		Key:        key,
		LowerBound: &Boundary{t.chunkPtr, true},
	}, nil
}

func (t *chunkerOptimistic) Next() (*Chunk, error) {
	t.Lock()
	defer t.Unlock()
	if t.IsRead() {
		return nil, ErrTableIsRead
	}
	if !t.isOpen {
		return nil, ErrTableNotOpen
	}

	// If there is a minimum value, we attempt to apply
	// the minimum value optimization.
	if t.chunkPtr.IsNil() {
		t.chunkPtr = t.Ti.minValue
		return &Chunk{
			ChunkSize:  t.chunkSize,
			Key:        t.Ti.KeyColumns[0],
			UpperBound: &Boundary{t.chunkPtr, false},
		}, nil
	}
	if t.chunkPrefetchingEnabled {
		return t.nextChunkByPrefetching()
	}

	// Before we return a final open bounded chunk, we check if the statistics
	// need updating, in which case we synchronously refresh them.
	// This helps reduce the risk of a very large unbounded
	// chunk from a table that is actively growing.
	if t.chunkPtr.GreaterThanOrEqual(t.Ti.maxValue) && t.Ti.statisticsNeedUpdating() {
		t.logger.Info("approaching the end of the table, synchronously updating statistics")
		if err := t.Ti.updateTableStatistics(context.TODO()); err != nil {
			return nil, err
		}
	}

	// Only now if there is a maximum value and the chunkPtr exceeds it, we apply
	// the maximum value optimization which is to return an open bounded
	// chunk.
	if t.chunkPtr.GreaterThanOrEqual(t.Ti.maxValue) {
		t.finalChunkSent = true
		return &Chunk{
			ChunkSize:  t.chunkSize,
			Key:        t.Ti.KeyColumns[0],
			LowerBound: &Boundary{t.chunkPtr, true},
		}, nil
	}

	// This is the typical case. We return a chunk with a lower bound
	// of the current chunkPtr and an upper bound of the chunkPtr + chunkSize,
	// but not exceeding math.MaxInt64.

	minVal := t.chunkPtr
	maxVal := t.chunkPtr.Add(t.chunkSize)
	t.chunkPtr = maxVal
	return &Chunk{
		ChunkSize:  t.chunkSize,
		Key:        t.Ti.KeyColumns[0],
		LowerBound: &Boundary{minVal, true},
		UpperBound: &Boundary{maxVal, false},
	}, nil
}

// Open opens a table to be used by NextChunk(). See also OpenAtWatermark()
// to resume from a specific point.
func (t *chunkerOptimistic) Open() (err error) {
	t.Lock()
	defer t.Unlock()

	return t.open()
}

func (t *chunkerOptimistic) setDynamicChunking(newValue bool) {
	t.Lock()
	defer t.Unlock()
	t.disableDynamicChunker = !newValue
}

func (t *chunkerOptimistic) OpenAtWatermark(cp string, highPtr datum) error {
	t.Lock()
	defer t.Unlock()

	// Open the table first.
	// This will reset the chunk pointer, but we'll set it before the mutex
	// is released.
	if err := t.open(); err != nil {
		return err
	}

	t.checkpointHighPtr = highPtr // set the high pointer.

	chunk, err := NewChunkFromJSON(cp, t.Ti.keyColumnsMySQLTp[0])
	if err != nil {
		return err
	}
	// We can restore from chunk.UpperBound, but because it is a < operator,
	// There might be an annoying off by 1 error. So let's just restore
	// from the chunk.LowerBound.
	t.watermark = chunk
	t.chunkPtr = chunk.LowerBound.Value
	return nil
}

func (t *chunkerOptimistic) Close() error {
	return nil
}

// Feedback is a way for consumers of chunks to give feedback on how long
// processing the chunk took. It is incorporated into the calculation of future
// chunk sizes.
func (t *chunkerOptimistic) Feedback(chunk *Chunk, d time.Duration) {
	t.Lock()
	defer t.Unlock()
	t.bumpWatermark(chunk)

	// Check if the feedback is based on an earlier chunker size.
	// if it is, it is misleading to incorporate feedback now.
	// We should just skip it. We also skip if dynamic chunking is disabled.
	if chunk.ChunkSize != t.chunkSize || t.disableDynamicChunker {
		return
	}

	// If any copyRows tasks take 5x the target size we reduce immediately
	// and don't wait for more feedback.
	if d > t.ChunkerTarget*DynamicPanicFactor {
		newTarget := uint64(float64(t.chunkSize) / float64(DynamicPanicFactor*2))
		t.logger.Infof("high chunk processing time. time: %s threshold: %s target-rows: %v target-ms: %v new-target-rows: %v",
			d,
			t.ChunkerTarget*DynamicPanicFactor,
			t.chunkSize,
			t.ChunkerTarget,
			newTarget,
		)
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
func (t *chunkerOptimistic) GetLowWatermark() (string, error) {
	t.Lock()
	defer t.Unlock()

	if t.watermark == nil || t.watermark.UpperBound == nil || t.watermark.LowerBound == nil {
		return "", errors.New("watermark not yet ready")
	}

	return t.watermark.JSON(), nil
}

// isSpecialRestoredChunk is used to test for the first chunk after restore-from-checkpoint.
// The restored chunk is a really special beast because the lowerbound
// will be repeated by the first chunk that is applied post restore.
// This is called under a mutex.
func (t *chunkerOptimistic) isSpecialRestoredChunk(chunk *Chunk) bool {
	if chunk.LowerBound == nil || chunk.UpperBound == nil || t.watermark == nil || t.watermark.LowerBound == nil || t.watermark.UpperBound == nil {
		return false // restored checkpoints always have both.
	}
	return chunk.LowerBound.Value == t.watermark.LowerBound.Value
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
func (t *chunkerOptimistic) bumpWatermark(chunk *Chunk) {
	// We never set the watermark for the very last chunk, which has an open ended upper bound.
	// This is safer anyway since between resumes a lot more data could arrive.
	if chunk.UpperBound == nil {
		return
	}
	// Check if this is the first chunk or it's the special restored chunk.
	// If so, set the watermark and then go on to applying any queued chunks.
	if (t.watermark == nil && chunk.LowerBound == nil) || t.isSpecialRestoredChunk(chunk) {
		t.watermark = chunk
		goto applyQueuedChunks
	}
	// We haven't set the first chunk yet, or it's not aligned with the
	// previous watermark. Queue it up, and move on.
	if t.watermark == nil || (t.watermark.UpperBound.Value != chunk.LowerBound.Value) {
		t.watermarkQueuedChunks = append(t.watermarkQueuedChunks, chunk)
		return
	}

	// The remaining case is:
	// t.watermark.UpperBound.Value == chunk.LowerBound.Value
	// Replace the current watermark with the chunk.
	t.watermark = chunk

applyQueuedChunks:

	for {
		// Check the queue for any chunks that align with the new watermark.
		// If there are any, pop them off the queue and bump the watermark.
		// If there are none, we're done.
		found := false
		for i, queuedChunk := range t.watermarkQueuedChunks {
			// sanity checking: chunks *should* have a lower bound and upper bound.
			if queuedChunk.LowerBound == nil || queuedChunk.UpperBound == nil {
				errMsg := fmt.Sprintf("chunkerOptimistic.bumpWatermark: nil value encountered: %v", queuedChunk)
				t.logger.Error(errMsg)
				panic(errMsg)
			}
			// The value aligns, remove it from the queued chunks and set the watermark to it.
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

func (t *chunkerOptimistic) open() (err error) {
	tp := mySQLTypeToDatumTp(t.Ti.keyColumnsMySQLTp[0])
	if tp == unknownType {
		return ErrUnsupportedPKType
	}
	if t.isOpen {
		// This prevents an error where open is re-called
		// leading to the watermark being in a strange state.
		return errors.New("table is already open, did you mean to call Reset()?")
	}
	t.isOpen = true
	t.chunkPtr = NewNilDatum(t.Ti.keyDatums[0])
	t.finalChunkSent = false
	t.chunkSize = StartingChunkSize

	// Make sure min/max value are always specified
	// To simplify the code in NextChunk funcs.
	if t.Ti.minValue.IsNil() {
		t.Ti.minValue = t.chunkPtr.MinValue()
	}
	if t.Ti.maxValue.IsNil() {
		t.Ti.maxValue = t.chunkPtr.MaxValue()
	}
	return nil
}

func (t *chunkerOptimistic) IsRead() bool {
	return t.finalChunkSent
}

func (t *chunkerOptimistic) updateChunkerTarget(newTarget uint64) {
	// Already called under a mutex.
	newTarget = t.boundaryCheckTargetChunkSize(newTarget)
	t.chunkSize = newTarget
	t.chunkTimingInfo = []time.Duration{}
}

func (t *chunkerOptimistic) boundaryCheckTargetChunkSize(newTarget uint64) uint64 {
	newTargetRows := float64(newTarget)

	// we only scale up 50% at a time in case the data from previous chunks had "gaps" leading to quicker than expected time.
	// this is for safety. If the chunks are really taking less time than our target, it will gradually increase chunk size
	if newTargetRows > float64(t.chunkSize)*MaxDynamicStepFactor {
		newTargetRows = float64(t.chunkSize) * MaxDynamicStepFactor
	}

	if newTargetRows > MaxDynamicRowSize {
		newTargetRows = MaxDynamicRowSize
	}

	if newTargetRows < MinDynamicRowSize {
		newTargetRows = MinDynamicRowSize
	}
	return uint64(newTargetRows)
}

func (t *chunkerOptimistic) calculateNewTargetChunkSize() uint64 {
	// We do all our math as float64 of time in ns
	p90 := float64(lazyFindP90(t.chunkTimingInfo))
	targetTime := float64(t.ChunkerTarget)
	newTargetRows := float64(t.chunkSize) * (targetTime / p90)
	// switch to prefetch chunking if:
	// - We are already at the max chunk size
	// - This new target wants to go higher
	// - our current p90 is only a fraction of our target time
	if t.chunkSize == MaxDynamicRowSize && newTargetRows > MaxDynamicRowSize && (p90*5 < targetTime) {
		t.logger.Warnf("dynamic chunking is not working as expected: target-time=%s p90-time=%s new-target-rows=%d max-dynamic-row-size=%d",
			time.Duration(targetTime), time.Duration(p90), uint64(newTargetRows), MaxDynamicRowSize,
		)
		t.logger.Warn("switching to prefetch algorithm")
		t.chunkSize = StartingChunkSize // reset
		t.chunkPrefetchingEnabled = true
	}
	return uint64(newTargetRows)
}

func (t *chunkerOptimistic) KeyAboveHighWatermark(key interface{}) bool {
	t.Lock()
	defer t.Unlock()
	if t.chunkPtr.IsNil() {
		return true // every key is above because we haven't started copying.
	}
	if t.finalChunkSent {
		return false // we're done, so everything is below.
	}
	keyDatum := newDatum(key, t.chunkPtr.tp)

	// If there is a checkpoint high pointer, first verify that
	// the key is above it. If it's not above it, we return FALSE
	// before we check the chunkPtr. This helps prevent a phantom
	// row issue.
	if !t.checkpointHighPtr.IsNil() && t.checkpointHighPtr.GreaterThanOrEqual(keyDatum) {
		return false
	}
	// Finally we check the chunkPtr.
	return keyDatum.GreaterThanOrEqual(t.chunkPtr)
}

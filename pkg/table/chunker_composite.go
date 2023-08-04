package table

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/siddontang/loggers"
)

type chunkerComposite struct {
	sync.Mutex
	Ti             *TableInfo
	chunkSize      uint64
	chunkPtrs      []Datum  // a list of Ptrs for each of the keys.
	chunkKeys      []string // all the keys to chunk on (usually all the col names of the PK)
	finalChunkSent bool
	isOpen         bool

	// Dynamic Chunking is time based instead of row based.
	// It uses *time* to determine the target chunk size.
	chunkTimingInfo []time.Duration
	ChunkerTarget   time.Duration // i.e. 500ms for target

	// This is used for restore.
	watermark             *Chunk
	watermarkQueuedChunks []*Chunk

	logger loggers.Advanced
}

var _ Chunker = &chunkerComposite{}

// Next in the composite chunker uses a query (aka prefetching) to determine the
// boundary of this chunk. This method is slower, but works better when the
// table can not predictably be chunked by just dividing the range between min and max values.
// as with auto_increment PRIMARY KEYs. This is the same method used by gh-ost.
func (t *chunkerComposite) Next() (*Chunk, error) {
	t.Lock()
	defer t.Unlock()
	if t.IsRead() {
		return nil, ErrTableIsRead
	}
	if !t.isOpen {
		return nil, ErrTableNotOpen
	}
	// Start prefetching the next chunk
	// First assume it's the first chunk, we can overwrite this
	// just below.
	query := fmt.Sprintf("SELECT %s FROM %s FORCE INDEX (PRIMARY) ORDER BY %s LIMIT 1 OFFSET %d",
		strings.Join(t.chunkKeys, ","),
		t.Ti.QuotedName,
		strings.Join(t.chunkKeys, ","),
		t.chunkSize,
	)
	if !t.isFirstChunk() {
		// This is not the first chunk, since we have pointers set.
		query = fmt.Sprintf("SELECT %s FROM %s FORCE INDEX (PRIMARY) WHERE %s ORDER BY %s LIMIT 1 OFFSET %d",
			strings.Join(t.chunkKeys, ","),
			t.Ti.QuotedName,
			expandRowConstructorComparison(t.chunkKeys, OpGreaterThan, t.chunkPtrs),
			strings.Join(t.chunkKeys, ","), // order by
			t.chunkSize,
		)
	}
	upperDatums, err := t.nextQueryToDatums(query)
	if err != nil {
		return nil, err
	}
	// Handle the special cases first:
	// there were no rows found, so we are at the end
	// of the table.
	if len(upperDatums) == 0 {
		t.finalChunkSent = true // This is the last chunk.
		if t.isFirstChunk() {   // and also the first chunk.
			return &Chunk{
				ChunkSize: t.chunkSize,
				Key:       t.chunkKeys,
			}, nil
		}
		// Else, it's just the last chunk.
		return &Chunk{
			ChunkSize:  t.chunkSize,
			Key:        t.chunkKeys,
			LowerBound: &Boundary{t.chunkPtrs, true},
		}, nil
	}
	// Else, there were rows found.
	// Convert upperVals to []Datum for the chunkPtrs.
	lowerBoundary := &Boundary{t.chunkPtrs, true}
	if t.isFirstChunk() {
		lowerBoundary = nil // first chunk
	}
	t.chunkPtrs = upperDatums
	return &Chunk{
		ChunkSize:  t.chunkSize,
		Key:        t.chunkKeys,
		LowerBound: lowerBoundary,
		UpperBound: &Boundary{upperDatums, false},
	}, nil
}

func (t *chunkerComposite) isFirstChunk() bool {
	return len(t.chunkPtrs) == 0
}

// nextQueryToDatums executes the prefetch query which returns 1 row-max.
// The columns in this result are then converted to Datums and returned
func (t *chunkerComposite) nextQueryToDatums(query string) ([]Datum, error) {
	rows, err := t.Ti.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columns := make([]sql.RawBytes, len(columnNames))
	columnPointers := make([]interface{}, len(columnNames))
	for i := 0; i < len(columnNames); i++ {
		columnPointers[i] = &columns[i]
	}
	rowsFound := false
	if rows.Next() {
		rowsFound = true
		err = rows.Scan(columnPointers...)
		if err != nil {
			return nil, err
		}
	}
	// If no rows were found we can early-return here.
	if !rowsFound {
		return nil, nil
	}
	// The types are currently broken because it scans as raw bytes.
	// We need to convert them to the correct type.
	var datums []Datum
	for i, name := range columnNames {
		newVal := reflect.ValueOf(columns[i]).Interface().(sql.RawBytes)
		datums = append(datums, newDatum(string(newVal), t.Ti.datumTp(name)))
	}
	return datums, nil
}

// Open opens a table to be used by NextChunk(). See also OpenAtWatermark()
// to resume from a specific point.
func (t *chunkerComposite) Open() (err error) {
	t.Lock()
	defer t.Unlock()

	return t.open()
}

// OpenAtWatermark opens a table for the resume-from-checkpoint use case.
// This will set the chunkPtr to a known safe value that is contained within
// the checkpoint. Because the composite chunker *does not support* the
// KeyAboveWatermark optimization, the second argument is ignored.
func (t *chunkerComposite) OpenAtWatermark(checkpnt string, _ Datum) error {
	t.Lock()
	defer t.Unlock()

	if err := t.open(); err != nil {
		return err
	}
	chunk, err := newChunkFromJSON(t.Ti, checkpnt)
	if err != nil {
		return err
	}
	// We can restore from chunk.UpperBound, but because it is a < operator,
	// There might be an annoying off by 1 error. So let's just restore
	// from the chunk.LowerBound.
	t.watermark = chunk
	t.chunkPtrs = chunk.LowerBound.Value
	return nil
}

func (t *chunkerComposite) Close() error {
	return nil
}

// Feedback is a way for consumers of chunks to give feedback on how long
// processing the chunk took. It is incorporated into the calculation of future
// chunk sizes.
func (t *chunkerComposite) Feedback(chunk *Chunk, d time.Duration) {
	t.Lock()
	defer t.Unlock()
	t.bumpWatermark(chunk)

	// Check if the feedback is based on an earlier chunker size.
	// if it is, it is misleading to incorporate feedback now.
	// We should just skip it.
	if chunk.ChunkSize != t.chunkSize {
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

	// If we have enough feedback, re-evaluate the chunk size.
	if len(t.chunkTimingInfo) > 10 {
		t.updateChunkerTarget(t.calculateNewTargetChunkSize())
	}
}

// GetLowWatermark returns the highest known value that has been safely copied,
// which (due to parallelism) could be significantly behind the high watermark.
// The value is discovered via ChunkerFeedback(), and when retrieved from this func
// can be used to write a checkpoint for restoration.
func (t *chunkerComposite) GetLowWatermark() (string, error) {
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
func (t *chunkerComposite) isSpecialRestoredChunk(chunk *Chunk) bool {
	if chunk.LowerBound == nil || chunk.UpperBound == nil || t.watermark == nil || t.watermark.LowerBound == nil || t.watermark.UpperBound == nil {
		return false // restored checkpoints always have both.
	}
	return chunk.LowerBound.comparesTo(t.watermark.LowerBound)
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
func (t *chunkerComposite) bumpWatermark(chunk *Chunk) {
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
	if t.watermark == nil || !t.watermark.UpperBound.comparesTo(chunk.LowerBound) {
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
			if queuedChunk.LowerBound.comparesTo(t.watermark.UpperBound) {
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

func (t *chunkerComposite) open() (err error) {
	if t.isOpen {
		// This prevents an error where open is re-called
		// leading to the watermark being in a strange state.
		return errors.New("table is already open, did you mean to call Reset()?")
	}
	t.isOpen = true
	t.chunkKeys = t.Ti.KeyColumns // chunk on all keys in the PK.
	t.finalChunkSent = false
	t.chunkSize = StartingChunkSize
	return nil
}

func (t *chunkerComposite) IsRead() bool {
	return t.finalChunkSent
}

func (t *chunkerComposite) updateChunkerTarget(newTarget uint64) {
	// Already called under a mutex.
	newTarget = t.boundaryCheckTargetChunkSize(newTarget)
	t.chunkSize = newTarget
	t.chunkTimingInfo = []time.Duration{}
}

func (t *chunkerComposite) boundaryCheckTargetChunkSize(newTarget uint64) uint64 {
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

func (t *chunkerComposite) calculateNewTargetChunkSize() uint64 {
	// We do all our math as float64 of time in ns
	p90 := float64(lazyFindP90(t.chunkTimingInfo))
	targetTime := float64(t.ChunkerTarget)
	newTargetRows := float64(t.chunkSize) * (targetTime / p90)
	return uint64(newTargetRows)
}

func (t *chunkerComposite) KeyAboveHighWatermark(key interface{}) bool {
	return false
}

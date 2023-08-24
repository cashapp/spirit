package table

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"golang.org/x/exp/slices"
)

type chunkerComposite struct {
	*coreChunker

	chunkSize uint64
	chunkPtrs []Datum  // a list of Ptrs for each of the keys.
	chunkKeys []string // all the keys to chunk on (usually all the col names of the PK)
	keyName   string   // the name of the key we are chunking on
	where     string   // any additional WHERE conditions.
}

var _ Chunker = &chunkerComposite{}

func (t *chunkerComposite) additionalConditionsSQL(whereSent bool) string {
	if t.where == "" {
		return ""
	}
	if whereSent {
		return fmt.Sprintf(" AND (%s)", t.where)
	}
	return " WHERE " + t.where
}

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
	query := fmt.Sprintf("SELECT %s FROM %s FORCE INDEX (%s) %s ORDER BY %s LIMIT 1 OFFSET %d",
		strings.Join(t.chunkKeys, ","),
		t.Ti.QuotedName,
		t.keyName,
		t.additionalConditionsSQL(false),
		strings.Join(t.chunkKeys, ","),
		t.chunkSize,
	)
	if !t.isFirstChunk() {
		// This is not the first chunk, since we have pointers set.
		query = fmt.Sprintf("SELECT %s FROM %s FORCE INDEX (%s) WHERE %s %s ORDER BY %s LIMIT 1 OFFSET %d",
			strings.Join(t.chunkKeys, ","),
			t.Ti.QuotedName,
			t.keyName,
			expandRowConstructorComparison(t.chunkKeys, OpGreaterThan, t.chunkPtrs),
			t.additionalConditionsSQL(true),
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
				ChunkSize:            t.chunkSize,
				Key:                  t.chunkKeys,
				AdditionalConditions: t.where,
			}, nil
		}
		// Else, it's just the last chunk.
		return &Chunk{
			ChunkSize:            t.chunkSize,
			Key:                  t.chunkKeys,
			LowerBound:           &Boundary{t.chunkPtrs, true},
			AdditionalConditions: t.where,
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
		ChunkSize:            t.chunkSize,
		Key:                  t.chunkKeys,
		LowerBound:           lowerBoundary,
		UpperBound:           &Boundary{upperDatums, false},
		AdditionalConditions: t.where,
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

func (t *chunkerComposite) open() (err error) {
	if t.isOpen {
		// This prevents an error where open is re-called
		// leading to the watermark being in a strange state.
		return errors.New("table is already open, did you mean to call Reset()?")
	}
	t.isOpen = true
	if len(t.chunkKeys) == 0 {
		// SetKey has not been called.
		// chunk on all keys in the PK.
		// default to primary key.
		t.chunkKeys = t.Ti.KeyColumns
		t.keyName = "PRIMARY"
	}
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

// SetKey allows you to chunk on a secondary index, and not the primary key.
// This is useful outside of the context of spirit, when the table package
// is used directly. It is only supported by the composite chunker,
// since the optimistic chunker is designed around auto_inc PKs.
func (t *chunkerComposite) SetKey(keyName string, where string) error {
	if t.isOpen {
		return errors.New("cannot set key after table is open")
	}
	keyCols, err := t.Ti.DescIndex(keyName)
	if err != nil {
		return err // index is not valid.
	}
	// There is a chance that if the index is something like "status" then it is low cardinality.
	// This is not ideal for chunking, and since we are allowed to assume InnoDB, each
	// secondary index actually includes the PRIMARY KEY columns in it.
	// So we can merge in the PK columns to the keyCols.
	// We only do this for each non-overlapping column in the PRIMARY KEY.
	// This is because ranging on the same column twice will create logic errors.
	for _, pkCol := range t.Ti.KeyColumns {
		if !slices.Contains(keyCols, pkCol) {
			keyCols = append(keyCols, pkCol)
		}
	}
	t.chunkKeys = keyCols
	t.keyName = keyName
	t.where = where
	return nil
}

// GetLowWatermark returns the highest known value that has been safely copied,
// which (due to parallelism) could be significantly behind the high watermark.
// The value is discovered via Chunker Feedback(), and when retrieved from this func
// can be used to write a checkpoint for restoration.
func (t *chunkerComposite) GetLowWatermark() (string, error) {
	t.Lock()
	defer t.Unlock()
	if t.watermark == nil || t.watermark.UpperBound == nil || t.watermark.LowerBound == nil {
		return "", errors.New("watermark not yet ready")
	}

	return t.watermark.JSON(), nil
}

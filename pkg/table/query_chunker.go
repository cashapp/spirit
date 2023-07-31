package table

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var _ Chunker = &QueryChunker{}

type QueryChunker struct {
	sync.Mutex
	Ti                *TableInfo
	Query             string
	chunkSize         uint64
	pageNum           uint64
	chunkPtr          Datum
	checkpointHighPtr Datum // the high watermark detected on restore
	finalChunkSent    bool
	isOpen            bool

	// Dynamic Chunking is time based instead of row based.
	// It uses *time* to determine the target chunk size.
	chunkTimingInfo []time.Duration
	ChunkerTarget   time.Duration // i.e. 500ms for target

	// This is used for restore.
	watermark             *Chunk
	watermarkQueuedChunks []*Chunk
}

func (q *QueryChunker) OpenAtWatermark(s string, datum Datum) error {
	//TODO implement me
	panic("implement me")
}

func (q *QueryChunker) Feedback(chunk *Chunk, duration time.Duration) {
	//TODO implement me
	panic("implement me")
}

func (q *QueryChunker) GetLowWatermark() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (q *QueryChunker) KeyAboveHighWatermark(i interface{}) bool {
	//TODO implement me
	panic("implement me")
}

func (t *QueryChunker) IsRead() bool {
	return t.finalChunkSent
}

func (t *QueryChunker) Next() (*Chunk, error) {
	t.Lock()
	defer t.Unlock()
	if t.IsRead() {
		return nil, ErrTableIsRead
	}
	if !t.isOpen {
		return nil, ErrTableNotOpen
	}
	key := t.Ti.KeyColumns[0]
	queryPart := fmt.Sprintf("%s ORDER BY %s LIMIT %d OFFSET %d", t.Query, key, t.chunkSize, t.chunkSize*t.pageNum)
	query := fmt.Sprintf("SELECT count(*) FROM (select %s from %s WHERE %s) t",
		key, t.Ti.QuotedName, queryPart,
	)
	fmt.Printf("CHunk query is:%s", query)

	var rowCount int
	err := t.Ti.db.QueryRow(query).Scan(&rowCount)
	if err != nil {
		return nil, err
	}
	if rowCount > 0 {
		t.pageNum++
		return &Chunk{
			Query:     queryPart,
			ChunkSize: t.chunkSize,
			Key:       key,
		}, nil
	}
	fmt.Println("No next rows for query %s", query)

	// Handle the case that there were no rows.
	// This means that < chunkSize rows remain in the table
	// and we processing the final chunk.
	t.finalChunkSent = true
	return nil, ErrTableIsRead
	// In the special case that chunkPtr is nil this means
	// we are processing the first *and* last chunk. i.e.
	// the table has less than chunkSize rows in it, so we
	// return a single chunk with no boundaries.
	//if t.chunkPtr.IsNil() {
	//	return &Chunk{
	//		ChunkSize: t.chunkSize,
	//		Key:       key,
	//	}, nil
	//}
	// Otherwise we return a final chunk with a lower bound
	// and an open ended upper bound.
	//query = fmt.Sprintf("SELECT MAX(%s) FROM %s WHERE %s ORDER BY %s",
	//	key, t.Ti.QuotedName, t.Query, key,
	//)
	//rows, err := t.Ti.db.Query(query)
	//if err != nil {
	//	return nil, err
	//}
	//defer rows.Close()
	//if rows.Next() {
	//	minVal := t.chunkPtr
	//	var upperVal string
	//	err = rows.Scan(&upperVal)
	//	if err != nil {
	//		return nil, err
	//	}
	//	lowerBoundary := &Boundary{minVal, true}
	//	if t.chunkPtr.IsNil() {
	//		lowerBoundary = nil // first chunk
	//	}
	//	upperBoundary := &Boundary{newDatum(upperVal, t.chunkPtr.Tp), true}
	//	return &Chunk{
	//		ChunkSize:  t.chunkSize,
	//		Key:        key,
	//		LowerBound: lowerBoundary,
	//		UpperBound: upperBoundary,
	//	}, nil
	//} else {
	//	return nil, errors.New("Unable to find max")
	//}
}

// Open opens a table to be used by NextChunk(). See also OpenAtWatermark()
// to resume from a specific point.
func (t *QueryChunker) Open() (err error) {
	t.Lock()
	defer t.Unlock()

	return t.open()
}
func (t *QueryChunker) open() (err error) {
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
	t.chunkSize = 10
	return nil
}

func (t *QueryChunker) Close() error {
	return nil
}

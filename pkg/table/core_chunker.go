package table

import (
	"fmt"
	"sync"
	"time"

	"github.com/siddontang/loggers"
)

// coreChunker contains the fields required by all chunkers and implements the methods
// that share functionality across all chunkers.
type coreChunker struct {
	sync.Mutex
	// This is used for restore.
	watermark *Chunk
	// Map from lowerbound value of a chunk -> chunk,
	// Used to update the watermark by applying stored chunks,
	// by comparing their lowerBound with current watermark upperBound.
	lowerBoundWatermarkMap map[string]*Chunk

	chunkSize uint64

	// Dynamic Chunking is time based instead of row based.
	// It uses *time* to determine the target chunk size.
	ChunkerTarget   time.Duration // i.e. 500ms for target
	chunkTimingInfo []time.Duration
	Ti              *TableInfo
	finalChunkSent  bool
	isOpen          bool
	logger          loggers.Advanced
}

type restorableChunker interface {
	isSpecialRestoredChunk(chunk *Chunk) bool
	bumpWatermark(chunk *Chunk)
}

var _ restorableChunker = &coreChunker{}

// isSpecialRestoredChunk is used to test for the first chunk after restore-from-checkpoint.
// The restored chunk is a really special beast because the lowerbound
// will be repeated by the first chunk that is applied post restore.
// This is called under a mutex.
func (t *coreChunker) isSpecialRestoredChunk(chunk *Chunk) bool {
	if chunk.LowerBound == nil || chunk.UpperBound == nil || t.watermark == nil || t.watermark.LowerBound == nil || t.watermark.UpperBound == nil {
		return false // restored checkpoints always have both.
	}
	return chunk.LowerBound.comparesTo(t.watermark.LowerBound)
}

// bumpWatermark updates the minimum value that is known to be safely copied,
// and is called under a mutex.
// Because of parallelism, it is possible that a chunk is copied out of order,
// so this func needs to account for that.
// Basically:
//   - If the chunk does not "align" to the current low watermark, it's stored in a map keyed by its lowerBound valuesString() value.
//   - If it does align, the watermark is bumped to the chunk's max value. Then
//     stored chunk map is checked to see if an existing chunk lowerBound aligns with the new watermark.
//   - If any stored chunk aligns, it is deleted off the map and the watermark is bumped.
//   - This process repeats until there is no more alignment from the stored map *or* the map is empty.
func (t *coreChunker) bumpWatermark(chunk *Chunk) {
	if chunk.UpperBound == nil {
		return
	}
	// Check if this is the first chunk or it's the special restored chunk.
	// If so, set the watermark and then go on to applying any stored chunks.
	if (t.watermark == nil && chunk.LowerBound == nil) || t.isSpecialRestoredChunk(chunk) {
		t.watermark = chunk
		goto applyStoredChunks
	}

	// Validate that chunk has lower bound before moving on
	if chunk.LowerBound == nil {
		errMsg := fmt.Sprintf("coreChunker.bumpWatermark: nil lowerBound value encountered more than once: %v", chunk)
		t.logger.Error(errMsg)
		panic(errMsg)
	}

	// We haven't set the first chunk yet, or it's not aligned with the
	// previous watermark. Store it in the map keyed by its lowerBound, and move on.

	// We only need to store by lowerBound because, when updating watermark
	// we always compare the upperBound of current watermark to lowerBound of stored chunks.
	// Key can never be nil, because first chunk will not hit this code path and all remaining chunks will have lowerBound.
	if t.watermark == nil || !t.watermark.UpperBound.comparesTo(chunk.LowerBound) {
		t.lowerBoundWatermarkMap[chunk.LowerBound.valuesString()] = chunk
		return
	}

	// The remaining case is:
	// t.watermark.UpperBound.Value == chunk.LowerBound.Value
	// Replace the current watermark with the chunk.
	t.watermark = chunk

applyStoredChunks:

	// Check the waterMarkMap for any chunks that align with the new watermark.
	// If there are any, bump the watermark and delete from the map.
	// If there are none, we're done.
	for t.waterMarkMapNotEmpty() && t.watermark.UpperBound != nil && t.lowerBoundWatermarkMap[t.watermark.UpperBound.valuesString()] != nil {
		nextWatermark := t.lowerBoundWatermarkMap[t.watermark.UpperBound.valuesString()]
		t.watermark = nextWatermark
		delete(t.lowerBoundWatermarkMap, nextWatermark.String())
	}
}

func (t *coreChunker) waterMarkMapNotEmpty() bool {
	return t.lowerBoundWatermarkMap != nil && len(t.lowerBoundWatermarkMap) != 0
}

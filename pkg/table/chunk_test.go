package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunk2String(t *testing.T) {
	chunk := &Chunk{
		Key: "id",
		LowerBound: &Boundary{
			Value:     newDatum(100, signedType),
			Inclusive: true,
		},
		UpperBound: &Boundary{
			Value:     newDatum(200, signedType),
			Inclusive: false,
		},
	}
	assert.Equal(t, "`id` >= 100 AND `id` < 200", chunk.String())
	chunk = &Chunk{
		Key: "id",
		LowerBound: &Boundary{
			Value:     newDatum(100, signedType),
			Inclusive: false,
		},
	}
	assert.Equal(t, "`id` > 100", chunk.String())
	chunk = &Chunk{
		Key: "id",
		UpperBound: &Boundary{
			Value:     newDatum(200, signedType),
			Inclusive: true,
		},
	}
	assert.Equal(t, "`id` <= 200", chunk.String())

	// Empty chunks are possible with the trivial chunker.
	chunk = &Chunk{
		Key: "id",
	}
	assert.Equal(t, "1=1", chunk.String())
}

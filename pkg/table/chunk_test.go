package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunk2String(t *testing.T) {
	chunk := &Chunk{
		Key: []string{"id"},
		LowerBound: &Boundary{
			Value:     []Datum{NewDatum(100, signedType)},
			Inclusive: true,
		},
		UpperBound: &Boundary{
			Value:     []Datum{NewDatum(200, signedType)},
			Inclusive: false,
		},
	}
	assert.Equal(t, "`id` >= 100 AND `id` < 200", chunk.String())
	chunk = &Chunk{
		Key: []string{"id"},
		LowerBound: &Boundary{
			Value:     []Datum{NewDatum(100, signedType)},
			Inclusive: false,
		},
	}
	assert.Equal(t, "`id` > 100", chunk.String())
	chunk = &Chunk{
		Key: []string{"id"},
		UpperBound: &Boundary{
			Value:     []Datum{NewDatum(200, signedType)},
			Inclusive: true,
		},
	}
	assert.Equal(t, "`id` <= 200", chunk.String())

	// Empty chunks are possible with the composite chunker
	chunk = &Chunk{
		Key: []string{"id"},
	}
	assert.Equal(t, "1=1", chunk.String())
}

func TestBoundary_ValueString(t *testing.T) {
	boundary1 := &Boundary{
		Value:     []Datum{NewDatum(100, signedType), NewDatum(200, signedType)},
		Inclusive: false,
	}
	assert.Equal(t, "\"100\",\"200\"", boundary1.valuesString())

	boundary2 := &Boundary{
		Value:     []Datum{NewDatum(100, signedType), NewDatum(200, signedType)},
		Inclusive: true,
	}
	// Tests that Inclusive doesn't matter between Boundaries for valuesString
	assert.Equal(t, boundary2.valuesString(), boundary1.valuesString())

	// Tests composite key boundary with mixed types
	boundary3 := &Boundary{
		Value: []Datum{NewDatum("PENDING", binaryType), NewDatum(2, signedType)},
	}
	assert.Equal(t, "\"PENDING\",\"2\"", boundary3.valuesString())
}

func TestCompositeChunks(t *testing.T) {
	chunk := &Chunk{
		Key: []string{"id1", "id2"},
		LowerBound: &Boundary{
			Value:     []Datum{NewDatum(100, signedType), NewDatum(200, signedType)},
			Inclusive: false,
		},
		UpperBound: &Boundary{
			Value:     []Datum{NewDatum(100, signedType), NewDatum(300, signedType)},
			Inclusive: false,
		},
	}
	assert.Equal(t, "((`id1` > 100)\n OR (`id1` = 100 AND `id2` > 200)) AND ((`id1` < 100)\n OR (`id1` = 100 AND `id2` < 300))", chunk.String())
	// 4 parts to the key - pretty unlikely.
	chunk = &Chunk{
		Key: []string{"id1", "id2", "id3", "id4"},
		LowerBound: &Boundary{
			Value:     []Datum{NewDatum(100, signedType), NewDatum(200, signedType), NewDatum(200, signedType), NewDatum(200, signedType)},
			Inclusive: true,
		},
		UpperBound: &Boundary{
			Value:     []Datum{NewDatum(101, signedType), NewDatum(12, signedType), NewDatum(123, signedType), NewDatum(1, signedType)},
			Inclusive: false,
		},
	}
	assert.Equal(t, "((`id1` > 100)\n OR (`id1` = 100 AND `id2` > 200)\n OR (`id1` = 100 AND `id2` = 200 AND `id3` > 200)\n OR (`id1` = 100 AND `id2` = 200 AND `id3` = 200 AND `id4` >= 200)) AND ((`id1` < 101)\n OR (`id1` = 101 AND `id2` < 12)\n OR (`id1` = 101 AND `id2` = 12 AND `id3` < 123)\n OR (`id1` = 101 AND `id2` = 12 AND `id3` = 123 AND `id4` < 1))", chunk.String())
	// A possible scenario when chunking on a non primary key is possible:
	chunk = &Chunk{
		Key: []string{"status", "id"},
		LowerBound: &Boundary{
			Value:     []Datum{NewDatum("ARCHIVED", binaryType), NewDatum(1234, signedType)},
			Inclusive: true,
		},
		UpperBound: &Boundary{
			Value:     []Datum{NewDatum("ARCHIVED", binaryType), NewDatum(5412, signedType)},
			Inclusive: false,
		},
	}
	assert.Equal(t, "((`status` > \"ARCHIVED\")\n OR (`status` = \"ARCHIVED\" AND `id` >= 1234)) AND ((`status` < \"ARCHIVED\")\n OR (`status` = \"ARCHIVED\" AND `id` < 5412))", chunk.String())
}

func TestComparesTo(t *testing.T) {
	b1 := &Boundary{
		Value:     []Datum{NewDatum(200, signedType)},
		Inclusive: true,
	}
	b2 := &Boundary{
		Value:     []Datum{NewDatum(200, signedType)},
		Inclusive: true,
	}
	assert.True(t, b1.comparesTo(b2))
	b2.Inclusive = false              // change operator
	assert.True(t, b1.comparesTo(b2)) // still compares
	b2.Value = []Datum{NewDatum(300, signedType)}
	assert.False(t, b1.comparesTo(b2))

	// Compound values.
	b1 = &Boundary{
		Value:     []Datum{NewDatum(200, signedType), NewDatum(300, signedType)},
		Inclusive: true,
	}
	b2 = &Boundary{
		Value:     []Datum{NewDatum(200, signedType), NewDatum(300, signedType)},
		Inclusive: true,
	}
	assert.True(t, b1.comparesTo(b2))
	b2.Value = []Datum{NewDatum(200, signedType), NewDatum(400, signedType)}
	assert.False(t, b1.comparesTo(b2))
}

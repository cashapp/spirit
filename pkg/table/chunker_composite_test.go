package table

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompositeChunkerBinary(t *testing.T) {
	runSQL(t, "DROP TABLE IF EXISTS composite_t1")
	runSQL(t, `CREATE TABLE composite_t1 (
		pk varbinary(40) NOT NULL,
		a int NOT NULL,
		b int NOT NULL,
		PRIMARY KEY (pk)
	)`)
	runSQL(t, `INSERT INTO composite_t1 (pk, a, b) SELECT UUID(), 1, 1 FROM dual`)
	runSQL(t, `INSERT INTO composite_t1 (pk, a, b) SELECT UUID(), 1, 1 FROM composite_t1 a JOIN composite_t1 b JOIN composite_t1 c LIMIT 1000000`)
	runSQL(t, `INSERT INTO composite_t1 (pk, a, b) SELECT UUID(), 1, 1 FROM composite_t1 a JOIN composite_t1 b JOIN composite_t1 c LIMIT 1000000`)
	runSQL(t, `INSERT INTO composite_t1 (pk, a, b) SELECT UUID(), 1, 1 FROM composite_t1 a JOIN composite_t1 b JOIN composite_t1 c LIMIT 1000000`)
	runSQL(t, `INSERT INTO composite_t1 (pk, a, b) SELECT UUID(), 1, 1 FROM composite_t1 a JOIN composite_t1 b JOIN composite_t1 c LIMIT 1000000`)

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "composite_t1")
	assert.NoError(t, t1.SetInfo(context.Background()))

	// Assert that the types are correct.
	assert.Equal(t, []string{"varbinary"}, t1.keyColumnsMySQLTp)
	assert.Equal(t, binaryType, t1.keyDatums[0])

	chunker, err := NewChunker(t1, ChunkerDefaultTarget, logrus.New())
	assert.NoError(t, err)
	assert.IsType(t, &chunkerComposite{}, chunker)

	assert.NoError(t, chunker.Open())

	chunk, err := chunker.Next()
	assert.NoError(t, err)
	assert.NotContains(t, "`pk` >= ", chunk.String()) // first chunk is special
	upperBound := chunk.UpperBound.Value.String()

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	previousUpperBound := upperBound
	upperBound = chunk.UpperBound.Value.String()
	require.NotEqual(t, previousUpperBound, upperBound)
	assert.Equal(t, fmt.Sprintf("`pk` >= %s AND `pk` < %s", previousUpperBound, upperBound), chunk.String())

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	previousUpperBound = upperBound
	upperBound = chunk.UpperBound.Value.String()
	require.NotEqual(t, previousUpperBound, upperBound)
	assert.Equal(t, fmt.Sprintf("`pk` >= %s AND `pk` < %s", previousUpperBound, upperBound), chunk.String())

	// Test it advances again
	chunk, err = chunker.Next()
	assert.NoError(t, err)
	previousUpperBound = upperBound
	upperBound = chunk.UpperBound.Value.String()
	require.NotEqual(t, previousUpperBound, upperBound)
	assert.Equal(t, fmt.Sprintf("`pk` >= %s AND `pk` < %s", previousUpperBound, upperBound), chunk.String())

	// Repeat until done (final chunk is sent.)
	// Add to the total chunks
	totalChunks := 3 // 3 so far

	for i := 0; i < 5000; i++ {
		chunk, err = chunker.Next()
		if err != nil {
			break
		}
		totalChunks++
		assert.NotNil(t, chunk)
	}
	// there are 1001010 rows. It should be about 1002 chunks.
	// we don't care that it's exact, since we don't want a flaky
	// test if we make small changes.
	assert.True(t, totalChunks < 1005 && totalChunks > 995)
}

func TestCompositeChunkerInt(t *testing.T) {
	runSQL(t, "DROP TABLE IF EXISTS compositeint_t1")
	runSQL(t, `CREATE TABLE compositeint_t1 (
		pk int NOT NULL primary key auto_increment,
		a int NOT NULL,
		b int NOT NULL
	)`)
	runSQL(t, `INSERT INTO compositeint_t1 (pk, a, b) SELECT NULL, 1, 1 FROM dual`)
	runSQL(t, `INSERT INTO compositeint_t1 (pk, a, b) SELECT NULL, 1, 1 FROM compositeint_t1 a JOIN compositeint_t1 b JOIN compositeint_t1 c LIMIT 1000000`)
	runSQL(t, `INSERT INTO compositeint_t1 (pk, a, b) SELECT NULL, 1, 1 FROM compositeint_t1 a JOIN compositeint_t1 b JOIN compositeint_t1 c LIMIT 1000000`)
	runSQL(t, `INSERT INTO compositeint_t1 (pk, a, b) SELECT NULL, 1, 1 FROM compositeint_t1 a JOIN compositeint_t1 b JOIN compositeint_t1 c LIMIT 1000000`)
	runSQL(t, `INSERT INTO compositeint_t1 (pk, a, b) SELECT NULL, 1, 1 FROM compositeint_t1 a JOIN compositeint_t1 b JOIN compositeint_t1 c LIMIT 1000000`)
	// remove autoinc before discovery.
	runSQL(t, "ALTER TABLE compositeint_t1 CHANGE COLUMN pk pk int NOT NULL") //nolint: dupword

	db, err := sql.Open("mysql", dsn())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "compositeint_t1")
	assert.NoError(t, t1.SetInfo(context.Background()))

	// Assert that the types are correct.
	assert.Equal(t, []string{"int"}, t1.keyColumnsMySQLTp)
	assert.Equal(t, signedType, t1.keyDatums[0])

	chunker, err := NewChunker(t1, ChunkerDefaultTarget, logrus.New())
	assert.NoError(t, err)
	assert.IsType(t, &chunkerComposite{}, chunker)

	assert.NoError(t, chunker.Open())

	// This might get messy if different versions skip
	// auto_inc values differently.

	chunk, err := chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`pk` < 1008", chunk.String()) // first chunk is special

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`pk` >= 1008 AND `pk` < 2032", chunk.String())

	chunk, err = chunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`pk` >= 2032 AND `pk` < 3033", chunk.String())

	totalChunks := 3 // 3 so far
	for i := 0; i < 5000; i++ {
		chunk, err = chunker.Next()
		if err != nil {
			break
		}
		totalChunks++
		assert.NotNil(t, chunk)
	}
	// there are 1001010 rows. It should be about 1002 chunks.
	// we don't care that it's exact, since we don't want a flaky
	// test if we make small changes.
	assert.True(t, totalChunks < 1005 && totalChunks > 995)
}

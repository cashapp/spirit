package table

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatum(t *testing.T) {
	signed := newDatum(1, signedType)
	unsigned := newDatum(uint(1), unsignedType)

	assert.Equal(t, "1", signed.String())
	assert.Equal(t, "1", unsigned.String())

	assert.Equal(t, fmt.Sprint(math.MinInt64), signed.MinValue().String())
	assert.Equal(t, fmt.Sprint(math.MaxInt64), signed.MaxValue().String())
	assert.Equal(t, "0", unsigned.MinValue().String())
	assert.Equal(t, "18446744073709551615", unsigned.MaxValue().String())

	newsigned := signed.Add(10)
	newunsigned := unsigned.Add(10)
	assert.Equal(t, "11", newsigned.String())
	assert.Equal(t, "11", newunsigned.String())

	assert.True(t, newsigned.GreaterThanOrEqual(signed))
	assert.True(t, newunsigned.GreaterThanOrEqual(unsigned))

	// Test for overflow on add operation.
	overflowSigned := newDatum(uint64(math.MaxInt64), signedType) // wrong type, converts.
	overflowUnsigned := newDatum(uint64(math.MaxUint64), unsignedType)
	assert.Equal(t, fmt.Sprint(math.MaxInt64), overflowSigned.Add(100).String())
	assert.Equal(t, "18446744073709551615", overflowUnsigned.Add(100).String())

	// Test unsigned with signed input
	unsigned = newDatum(int(1), unsignedType)
	assert.Equal(t, "1", unsigned.String())

	// Test binary type.
	binary := newDatum("0", binaryType)
	assert.Equal(t, "0x0", binary.String())
	binary = binary.Add(1000)
	assert.Equal(t, "0x3E8", binary.String())
}

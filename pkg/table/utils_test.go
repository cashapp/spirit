package table

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHexString(t *testing.T) {
	assert.Equal(t, "0x0", hexString(0))
	assert.Equal(t, "0xFF", hexString(255))
}

func TestHexStringToUint64(t *testing.T) {
	uintVal, err := hexStringToUint64("0x0")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), uintVal)

	uintVal, err = hexStringToUint64("0xFF")
	assert.NoError(t, err)
	assert.Equal(t, uint64(255), uintVal)

	_, err = hexStringToUint64("FFfds")
	assert.Error(t, err)
}

func TestFindP90(t *testing.T) {
	times := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		1 * time.Second,
		3 * time.Second,
		10 * time.Second,
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
	}
	assert.Equal(t, 3*time.Second, lazyFindP90(times))
}

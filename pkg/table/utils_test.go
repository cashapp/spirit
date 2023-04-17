package table

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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

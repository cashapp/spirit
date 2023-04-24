package table

import (
	"sort"
	"time"
)

// lazyFindP90 finds the second to last value in a slice.
// This is the same as a p90 if there are 10 values, but if
// there were 100 values it would technically be a p99 etc.
func lazyFindP90(a []time.Duration) time.Duration {
	sort.Slice(a, func(i, j int) bool {
		return a[i] > a[j]
	})
	return a[len(a)/10]
}

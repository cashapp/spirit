package check

import (
	"context"

	"github.com/siddontang/loggers"
)

func init() {
	registerCheck("memcomparable", memcomparableCheck, ScopePreflight)
}

// memcomparableCheck checks if the key is memory comparable.
// This is required for spirit's Delta Map, but
// not specifically required for the chunker.
func memcomparableCheck(ctx context.Context, r Resources, logger loggers.Advanced) error {
	return r.Table.PrimaryKeyIsMemoryComparable()
}

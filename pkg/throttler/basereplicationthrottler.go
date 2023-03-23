package throttler

import (
	"database/sql"
	"sync/atomic"
	"time"

	"github.com/siddontang/loggers"
)

type BaseReplicationThrottler struct {
	replica          *sql.DB
	lagToleranceInMs int64
	currentLagInMs   int64
	logger           loggers.Advanced
}

func (l *BaseReplicationThrottler) IsThrottled() bool {
	return atomic.LoadInt64(&l.currentLagInMs) >= l.lagToleranceInMs
}

// BlockWait blocks until the lag is within the tolerance, or up to 60s
// to allow some progress to be made.
func (l *BaseReplicationThrottler) BlockWait() {
	for i := 0; i < 60; i++ {
		if atomic.LoadInt64(&l.currentLagInMs) < l.lagToleranceInMs {
			return
		}
		time.Sleep(1 * time.Second)
	}
	l.logger.Warnf("lag monitor timed out. lag: %v tolerance: %v", atomic.LoadInt64(&l.currentLagInMs), l.lagToleranceInMs)
}

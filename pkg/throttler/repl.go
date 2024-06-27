package throttler

import (
	"database/sql"
	"sync/atomic"
	"time"

	"github.com/siddontang/loggers"
)

var blockWaitInterval = 1 * time.Second

type Repl struct {
	replica        *sql.DB
	lagTolerance   time.Duration
	currentLagInMs int64
	logger         loggers.Advanced
}

func (l *Repl) IsThrottled() bool {
	return atomic.LoadInt64(&l.currentLagInMs) >= l.lagTolerance.Milliseconds()
}

// BlockWait blocks until the lag is within the tolerance, or up to 60s
// to allow some progress to be made.
func (l *Repl) BlockWait() {
	for range 60 {
		if atomic.LoadInt64(&l.currentLagInMs) < l.lagTolerance.Milliseconds() {
			return
		}
		time.Sleep(blockWaitInterval)
	}
	l.logger.Warnf("lag monitor timed out. lag: %v tolerance: %v", atomic.LoadInt64(&l.currentLagInMs), l.lagTolerance)
}

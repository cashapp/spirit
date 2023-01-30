package throttler

import (
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

type MySQL80Replica struct {
	replica          *sql.DB
	lagToleranceInMs int64
	currentLagInMs   int64
	logger           log.FieldLogger
}

var _ Throttler = &MySQL80Replica{}

// Start starts the lag monitor. This is not gh-ost. The lag monitor is primitive
// because the requirement is only for DR, and not for up-to-date read-replicas.
// Because chunk-sizes are typically 500ms, getting fine-grained metrics is not realistic.
// We only check the replica every 5 seconds, and typically allow up to 120s
// of replica lag, which is a lot.
func (l *MySQL80Replica) Start() error {
	if err := l.UpdateLag(); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if err := l.UpdateLag(); err != nil {
				l.logger.WithFields(log.Fields{
					"error": err,
				}).Error("error getting lag")
			}
		}
	}()
	return nil
}

// UpdateLag is a MySQL 8.0+ implementation of lag that is a better approximation than "seconds_behind_master".
// It uses the most up to date TS of the master's commit, and compares that generously to the most up to date
// TS of the replicas commit. Because of multi-threaded replicas, seconds behind master is out of date. It also
// has weaknesses if there are any delays in commits, since it's technically the TIMESTAMP value of the statement.
// An alternative is to use a Heatbeat table, but that's way more complicated than this code wants to get into.
func (l *MySQL80Replica) UpdateLag() error {
	query := `SELECT CEIL(TIMESTAMPDIFF(MICROSECOND,
 max(LAST_APPLIED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP),
 max(LAST_APPLIED_TRANSACTION_END_APPLY_TIMESTAMP)
 )/1000) as lagMs
 FROM performance_schema.replication_applier_status_by_worker`
	var newLagValue int64
	if err := l.replica.QueryRow(query).Scan(&newLagValue); err != nil {
		return fmt.Errorf("could not check replication lag, check that this is a MySQL 8.0 replica, and that performance_schema is enabled")
	}
	atomic.StoreInt64(&l.currentLagInMs, newLagValue)
	if l.IsThrottled() {
		l.logger.WithFields(log.Fields{
			"lag":       atomic.LoadInt64(&l.currentLagInMs),
			"tolerance": l.lagToleranceInMs,
		}).Warn("replication delayed, copier is now being throttled")
	}
	return nil
}

func (l *MySQL80Replica) IsThrottled() bool {
	return atomic.LoadInt64(&l.currentLagInMs) >= l.lagToleranceInMs
}

// BlockWait blocks until the lag is within the tolerance, or up to 60s
// to allow some progress to be made.
func (l *MySQL80Replica) BlockWait() {
	for i := 0; i < 60; i++ {
		if atomic.LoadInt64(&l.currentLagInMs) < l.lagToleranceInMs {
			return
		}
		time.Sleep(1 * time.Second)
	}
	l.logger.WithFields(log.Fields{
		"lag":       atomic.LoadInt64(&l.currentLagInMs),
		"tolerance": l.lagToleranceInMs,
	}).Warn("lag monitor timed out")
}

package throttler

import (
	"database/sql"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type MySQL57Replica struct {
	sync.Mutex
	Repl
	isClosed atomic.Bool
}

var _ Throttler = &MySQL57Replica{}

// Open starts the lag monitor. This is not gh-ost. The lag monitor is primitive
// because the requirement is only for DR, and not for up-to-date read-replicas.
// Because chunk-sizes are typically 500ms, getting fine-grained metrics is not realistic.
// We only check the replica every 5 seconds, and typically allow up to 120s
// of replica lag, which is a lot.
func (l *MySQL57Replica) Open() error {
	if err := l.UpdateLag(); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(loopInterval)
		defer ticker.Stop()
		for range ticker.C {
			if l.isClosed.Load() {
				return
			}
			if err := l.UpdateLag(); err != nil {
				l.logger.Errorf("error getting lag: %s", err.Error())
			}
		}
	}()
	return nil
}

func (l *MySQL57Replica) Close() error {
	l.isClosed.Store(true)
	return nil
}

// UpdateLag is a legacy MySQL 5.7 implementation of replica lag which uses Seconds_Behind_master.
// This is because 5.7 does not have the required lag columns on
// performance_schema.replication_applier_status_by_worker.
func (l *MySQL57Replica) UpdateLag() error {
	res, err := l.replica.Query(`SHOW SLAVE STATUS`) //nolint: execinquery
	if err != nil {
		return err
	}
	defer res.Close()
	cols, err := res.Columns()
	if err != nil {
		return err
	}
	for res.Next() {
		scanArgs := make([]interface{}, len(cols))
		for i := range scanArgs {
			scanArgs[i] = &sql.RawBytes{}
		}
		if err := res.Scan(scanArgs...); err != nil {
			return err
		}
		// Fetch seconds behind master
		newLagStr := columnValue(scanArgs, cols, "Seconds_Behind_Master")
		newLag, err := strconv.Atoi(newLagStr)
		if err != nil {
			return errors.New("replica lag could not be read. is it possible replication is stopped?") // possibly a NULL value?
		}
		// Store the new value.
		atomic.StoreInt64(&l.currentLagInMs, int64(newLag*1000))
		if l.IsThrottled() {
			l.logger.Warnf("replication delayed, copier is now being throttled. lag: %v tolerance: %v", atomic.LoadInt64(&l.currentLagInMs), l.lagTolerance)
		}
	}
	return nil
}

func columnValue(scanArgs []interface{}, slaveCols []string, colName string) string {
	var columnIndex = columnIndex(slaveCols, colName)
	if columnIndex == -1 {
		return ""
	}
	return string(*scanArgs[columnIndex].(*sql.RawBytes))
}

func columnIndex(slaveCols []string, colName string) int {
	for idx := range slaveCols {
		if slaveCols[idx] == colName {
			return idx
		}
	}
	return -1
}

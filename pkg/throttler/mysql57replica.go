package throttler

import (
	"database/sql"
	"fmt"
	"strconv"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

type MySQL57Replica struct {
	MySQL80Replica
}

var _ Throttler = &MySQL57Replica{}

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
			return fmt.Errorf("replica lag could not be read. is it possible replication is stopped?") // possibly a NULL value?
		}
		// Store the new value.
		atomic.StoreInt64(&l.currentLagInMs, int64(newLag*1000))
		if l.IsThrottled() {
			l.logger.WithFields(log.Fields{
				"lag":       atomic.LoadInt64(&l.currentLagInMs),
				"tolerance": l.lagToleranceInMs,
			}).Warn("replication delayed, copier is now being throttled")
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

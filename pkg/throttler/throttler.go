// Package throttler contains code to throttle the rate of writes to a table.
package throttler

import (
	"database/sql"

	log "github.com/sirupsen/logrus"
)

type Throttler interface {
	Start() error
	IsThrottled() bool
	BlockWait()
	UpdateLag() error
}

// NewReplicationThrottler returns a Throttler that is appropriate for the
// current replica. It will return a MySQL80Replica throttler if the version is detected
// as 8.0, and a MySQL57Replica throttler otherwise.
func NewReplicationThrottler(replica *sql.DB, lagToleranceInMs int64, logger log.FieldLogger) (Throttler, error) {
	var version string
	if err := replica.QueryRow("select substr(version(), 1, 1)").Scan(&version); err != nil {
		return nil, err
	}
	if version == "8" {
		return &MySQL80Replica{
			replica:          replica,
			lagToleranceInMs: lagToleranceInMs,
			logger:           logger,
		}, nil
	}
	return &MySQL57Replica{
		MySQL80Replica{
			replica:          replica,
			lagToleranceInMs: lagToleranceInMs,
			logger:           logger,
		},
	}, nil
}

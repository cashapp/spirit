// Package throttler contains code to throttle the rate of writes to a table.
package throttler

import (
	"database/sql"

	"github.com/siddontang/loggers"
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
// It returns an error if querying for either fails, i.e. it might not be a valid DB connection.
func NewReplicationThrottler(replica *sql.DB, lagToleranceInMs int64, logger loggers.Advanced) (Throttler, error) {
	var version string
	if err := replica.QueryRow("select substr(version(), 1, 1)").Scan(&version); err != nil {
		return nil, err
	}
	if version == "8" {
		return &MySQL80Replica{
			Repl{
				replica:          replica,
				lagToleranceInMs: lagToleranceInMs,
				logger:           logger,
			},
		}, nil
	}
	return &MySQL57Replica{
		Repl{
			replica:          replica,
			lagToleranceInMs: lagToleranceInMs,
			logger:           logger,
		},
	}, nil
}

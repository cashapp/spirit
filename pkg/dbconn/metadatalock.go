package dbconn

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cashapp/spirit/pkg/dbconn/sqlescape"
	"github.com/siddontang/loggers"
)

var (
	// getLockTimeout is the timeout for acquiring the GET_LOCK. We set it to 0
	// because we want to return immediately if the lock is not available
	getLockTimeout  = 0 * time.Second
	refreshInterval = 1 * time.Minute
)

type MetadataLock struct {
	cancel          context.CancelFunc
	closeCh         chan error
	refreshInterval time.Duration
}

func NewMetadataLock(ctx context.Context, dsn string, lockName string, logger loggers.Advanced, optionFns ...func(*MetadataLock)) (*MetadataLock, error) {
	if len(lockName) == 0 {
		return nil, errors.New("metadata lock name is empty")
	}
	if len(lockName) > 64 {
		return nil, fmt.Errorf("metadata lock name is too long: %d, max length is 64", len(lockName))
	}

	mdl := &MetadataLock{
		refreshInterval: refreshInterval,
	}

	// Apply option functions
	for _, optionFn := range optionFns {
		optionFn(mdl)
	}

	// Setup the dedicated connection for this lock
	dbConfig := NewDBConfig()
	dbConfig.MaxOpenConnections = 1
	dbConn, err := New(dsn, dbConfig)
	if err != nil {
		return nil, err
	}

	// Function to acquire the lock
	getLock := func() error {
		// https://dev.mysql.com/doc/refman/8.0/en/locking-functions.html#function_get-lock
		var answer int
		stmt := sqlescape.MustEscapeSQL("SELECT GET_LOCK(%?, %?)", lockName, getLockTimeout.Seconds())
		if err := dbConn.QueryRowContext(ctx, stmt).Scan(&answer); err != nil {
			return fmt.Errorf("could not acquire metadata lock: %s", err)
		}
		if answer == 0 {
			// 0 means the lock is held by another connection
			// TODO: we could lookup the connection that holds the lock and report details about it
			return fmt.Errorf("could not acquire metadata lock: %s, lock is held by another connection", lockName)
		} else if answer != 1 {
			// probably we never get here, but just in case
			return fmt.Errorf("could not acquire metadata lock: %s, GET_LOCK returned: %d", lockName, answer)
		}
		return nil
	}

	// Acquire the lock or return an error immediately
	// We only Infof the initial acquisition.
	logger.Infof("attempting to acquire metadata lock: %s", lockName)
	if err = getLock(); err != nil {
		return nil, err
	}
	logger.Infof("acquired metadata lock: %s", lockName)

	// Setup background refresh runner
	ctx, mdl.cancel = context.WithCancel(ctx)
	mdl.closeCh = make(chan error)
	go func() {
		ticker := time.NewTicker(mdl.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// Close the dedicated connection to release the lock
				logger.Warnf("releasing metadata lock: %s", lockName)
				mdl.closeCh <- dbConn.Close()
				return
			case <-ticker.C:
				if err = getLock(); err != nil {
					// if we can't refresh the lock, it's okay.
					// We have other safety mechanisms in place to prevent corruption
					// for example, we watch the binary log to see metadata changes
					// that we did not make. This makes it a warning, not an error,
					// and we can try again on the next tick interval.
					logger.Warnf("could not refresh metadata lock: %s", err)
				} else {
					logger.Debugf("refreshed metadata lock: %s", lockName)
				}
			}
		}
	}()

	return mdl, nil
}

func (m *MetadataLock) Close() error {
	// Cancel the background refresh runner
	m.cancel()

	// Wait for the dedicated connection to be closed and return its error (if any)
	return <-m.closeCh
}

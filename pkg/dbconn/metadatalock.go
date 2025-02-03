package dbconn

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cashapp/spirit/pkg/table"

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
	ticker          *time.Ticker
	dbConn          *sql.DB
}

func NewMetadataLock(ctx context.Context, dsn string, table *table.TableInfo, logger loggers.Advanced, optionFns ...func(*MetadataLock)) (*MetadataLock, error) {
	if table == nil {
		return nil, errors.New("metadata lock table info is nil")
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
	mdl.dbConn = dbConn

	// Function to acquire the lock
	getLock := func() error {
		// https://dev.mysql.com/doc/refman/8.0/en/locking-functions.html#function_get-lock
		var answer int
		// Using the table name alone entails a maximum lock length and leads to conflicts
		// between different tables with the same name in different schemas.
		// We use the schema name and table name to create a unique lock name with a hash.
		// The hash is truncated to 8 characters to avoid the maximum lock length.
		// bizarrely_long_schema_name.thisisareallylongtablenamethisisareallylongtablename60charac ==>
		// bizarrely_long_schem.thisisareallylongtablenamethisis-66fec116
		//
		// The computation of the hash is done server-side to simplify the whole process,
		// but that means we can't easily log the actual lock name used. If you want to do that
		// in the future, just add another MySQL round-trip to compute the lock name server-side
		// and then use the returned string in the GET_LOCK call.
		stmt := sqlescape.MustEscapeSQL("SELECT GET_LOCK( concat(left(%?,20),'.',left(%?,32),'-',left(sha1(concat(%?,%?)),8)), %?)", table.SchemaName, table.TableName, table.SchemaName, table.TableName, getLockTimeout.Seconds())
		if err := dbConn.QueryRowContext(ctx, stmt).Scan(&answer); err != nil {
			return fmt.Errorf("could not acquire metadata lock: %s", err)
		}
		if answer == 0 {
			// 0 means the lock is held by another connection
			// TODO: we could lookup the connection that holds the lock and report details about it
			return fmt.Errorf("could not acquire metadata lock for %s.%s, lock is held by another connection", table.SchemaName, table.TableName)
		} else if answer != 1 {
			// probably we never get here, but just in case
			return fmt.Errorf("could not acquire metadata lock for %s.%s, GET_LOCK returned: %d", table.SchemaName, table.TableName, answer)
		}
		return nil
	}

	// Acquire the lock or return an error immediately
	// We only Infof the initial acquisition.
	logger.Infof("attempting to acquire metadata lock for %s.%s", table.SchemaName, table.TableName)
	if err = getLock(); err != nil {
		return nil, err
	}
	logger.Infof("acquired metadata lock: %s.%s", table.SchemaName, table.TableName)

	// Setup background refresh runner
	ctx, mdl.cancel = context.WithCancel(ctx)
	mdl.closeCh = make(chan error)
	go func() {
		mdl.ticker = time.NewTicker(mdl.refreshInterval)
		defer mdl.ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// Close the dedicated connection to release the lock
				logger.Warnf("releasing metadata lock for %s.%s", table.SchemaName, table.TableName)
				mdl.closeCh <- dbConn.Close()
				return
			case <-mdl.ticker.C:
				if err = getLock(); err != nil {
					// if we can't refresh the lock, it's okay.
					// We have other safety mechanisms in place to prevent corruption
					// for example, we watch the binary log to see metadata changes
					// that we did not make. This makes it a warning, not an error,
					// and we can try again on the next tick interval.
					logger.Warnf("could not refresh metadata lock: %s", err)
				} else {
					logger.Debugf("refreshed metadata lock for %s.%s", table.SchemaName, table.TableName)
				}
			}
		}
	}()

	return mdl, nil
}

func (m *MetadataLock) Close() error {
	// Handle odd race situation here where the cancel func is nil somehow
	if m.cancel == nil {
		// Make a best effort to cleanup
		if m.ticker != nil {
			m.ticker.Stop()
		}
		if m.closeCh != nil {
			close(m.closeCh)
		}
		if m.dbConn != nil {
			return m.dbConn.Close()
		}
		return nil
	}

	// Cancel the background refresh runner
	m.cancel()

	// Wait for the dedicated connection to be closed and return its error (if any)
	return <-m.closeCh
}

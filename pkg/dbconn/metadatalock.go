package dbconn

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
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
	closeTimeout    time.Duration
	db              *sql.DB
	lockName        string
}

// WithCloseTimeout sets the timeout for close operations
func WithCloseTimeout(timeout time.Duration) func(*MetadataLock) {
	return func(m *MetadataLock) {
		m.closeTimeout = timeout
	}
}

// WithRefreshInterval sets the interval for lock refresh operations
func WithRefreshInterval(interval time.Duration) func(*MetadataLock) {
	return func(m *MetadataLock) {
		m.refreshInterval = interval
	}
}

func NewMetadataLock(ctx context.Context, dsn string, table *table.TableInfo, logger loggers.Advanced, optionFns ...func(*MetadataLock)) (*MetadataLock, error) {
	if table == nil {
		return nil, errors.New("metadata lock table info is nil")
	}

	mdl := &MetadataLock{
		refreshInterval: refreshInterval,
		closeTimeout:    30 * time.Second,
		closeCh:         make(chan error, 1), // Buffered channel to prevent goroutine leaks
	}

	// Apply option functions
	for _, optionFn := range optionFns {
		optionFn(mdl)
	}

	// Setup the dedicated connection for this lock
	dbConfig := NewDBConfig()
	dbConfig.MaxOpenConnections = 1
	var err error
	mdl.db, err = New(dsn, dbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create database connection: %w", err)
	}

	// Function to acquire the lock
	getLock := func() error {
		if mdl.db == nil {
			return errors.New("database connection is nil")
		}

		var answer int
		mdl.lockName = computeLockName(table)
		stmt := sqlescape.MustEscapeSQL("SELECT GET_LOCK(%?, %?)", mdl.lockName, getLockTimeout.Seconds())

		// Use context with timeout for the query
		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		if err := mdl.db.QueryRowContext(queryCtx, stmt).Scan(&answer); err != nil {
			return fmt.Errorf("could not acquire metadata lock: %w", err)
		}

		if answer == 0 {
			return fmt.Errorf("could not acquire metadata lock for %s, lock is held by another connection", mdl.lockName)
		} else if answer != 1 {
			return fmt.Errorf("could not acquire metadata lock %s, GET_LOCK returned: %d", mdl.lockName, answer)
		}
		return nil
	}

	// Acquire the lock or return an error immediately
	logger.Infof("attempting to acquire metadata lock %s", computeLockName(table))
	if err = getLock(); err != nil {
		mdl.db.Close() // Clean up the connection if we fail to get the lock
		return nil, err
	}
	logger.Infof("acquired metadata lock: %s", mdl.lockName)

	// Setup background refresh runner
	ctx, mdl.cancel = context.WithCancel(ctx)

	go func() {
		ticker := time.NewTicker(mdl.refreshInterval)
		defer ticker.Stop()
		defer func() {
			if mdl.db != nil {
				err := mdl.db.Close()
				select {
				case mdl.closeCh <- err:
				default:
					// Channel is full or closed, log the error
					logger.Errorf("failed to send close error: %v", err)
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				logger.Infof("releasing metadata lock: %s", mdl.lockName)
				return

			case <-ticker.C:
				if err = getLock(); err != nil {
					logger.Warnf("could not refresh metadata lock: %s", err)

					// Create new connection before closing old one
					newDB, newErr := New(dsn, dbConfig)
					if newErr != nil {
						logger.Warnf("could not establish new database connection: %s", newErr)
						continue
					}

					// Close old connection
					if mdl.db != nil {
						if closeErr := mdl.db.Close(); closeErr != nil {
							logger.Warnf("could not close old database connection: %s", closeErr)
						}
					}

					// Set new connection and try to acquire lock
					mdl.db = newDB
					if err = getLock(); err != nil {
						logger.Warnf("could not acquire metadata lock after re-establishing connection: %s", err)
						continue
					}

					logger.Infof("re-acquired metadata lock after re-establishing connection: %s", mdl.lockName)
				} else {
					logger.Debugf("refreshed metadata lock for %s", mdl.lockName)
				}
			}
		}
	}()

	return mdl, nil
}

func (m *MetadataLock) Close() error {
	if m.cancel != nil {
		m.cancel() // Cancel the context first
	}

	// Add timeout to prevent hanging
	select {
	case err := <-m.closeCh:
		return err
	case <-time.After(m.closeTimeout):
		// Force close the DB connection if we timeout
		if m.db != nil {
			if err := m.db.Close(); err != nil {
				return fmt.Errorf("forced close after timeout failed: %w", err)
			}
			m.db = nil
		}
		return fmt.Errorf("metadata lock close operation timed out after %v", m.closeTimeout)
	}
}

func (m *MetadataLock) CloseDBConnection(logger loggers.Advanced) error {
	logger.Infof("About to close MetadataLock database connection")
	if m.db != nil {
		err := m.db.Close()
		m.db = nil // Ensure we don't try to close it again
		return err
	}
	return nil
}

func (m *MetadataLock) GetLockName() string {
	return m.lockName
}

func computeLockName(table *table.TableInfo) string {
	schemaNamePart := table.SchemaName
	if len(schemaNamePart) > 20 {
		schemaNamePart = schemaNamePart[:20]
	}

	tableNamePart := table.TableName
	if len(tableNamePart) > 32 {
		tableNamePart = tableNamePart[:32]
	}

	hash := sha1.New()
	hash.Write([]byte(table.SchemaName + table.TableName))
	hashPart := hex.EncodeToString(hash.Sum(nil))[:8]

	return fmt.Sprintf("%s.%s-%s", schemaNamePart, tableNamePart, hashPart)
}

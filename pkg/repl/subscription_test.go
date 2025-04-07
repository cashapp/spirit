package repl

import (
	"testing"

	"github.com/cashapp/spirit/pkg/dbconn"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/cashapp/spirit/pkg/testutils"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func setupTestTables(t *testing.T) (*table.TableInfo, *table.TableInfo) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS subscription_test, _subscription_test_new`)
	testutils.RunSQL(t, `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	srcTable := table.NewTableInfo(db, "test", "subscription_test")
	dstTable := table.NewTableInfo(db, "test", "_subscription_test_new")

	err = srcTable.SetInfo(t.Context())
	assert.NoError(t, err)
	err = dstTable.SetInfo(t.Context())
	assert.NoError(t, err)

	return srcTable, dstTable
}

func TestSubscriptionDeltaMap(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &subscription{
		c:          client,
		table:      srcTable,
		newTable:   dstTable,
		deltaMap:   make(map[string]bool),
		deltaQueue: nil,
	}

	// Test initial state
	assert.Equal(t, 0, sub.getDeltaLen())

	// Test key changes
	sub.keyHasChanged([]interface{}{1}, false) // Insert/Replace
	assert.Equal(t, 1, sub.getDeltaLen())

	sub.keyHasChanged([]interface{}{2}, true) // Delete
	assert.Equal(t, 2, sub.getDeltaLen())

	// Test statement generation
	deleteStmt := sub.createDeleteStmt([]string{"'1'"})
	assert.Contains(t, deleteStmt.stmt, "DELETE FROM")
	assert.Contains(t, deleteStmt.stmt, "WHERE")
	assert.Equal(t, 1, deleteStmt.numKeys)

	replaceStmt := sub.createReplaceStmt([]string{"'1'"})
	assert.Contains(t, replaceStmt.stmt, "REPLACE INTO")
	assert.Contains(t, replaceStmt.stmt, "SELECT")
	assert.Equal(t, 1, replaceStmt.numKeys)
}

func TestSubscriptionDeltaQueue(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &subscription{
		c:               client,
		table:           srcTable,
		newTable:        dstTable,
		deltaMap:        nil,
		deltaQueue:      make([]queuedChange, 0),
		disableDeltaMap: true,
	}

	// Test initial state
	assert.Equal(t, 0, sub.getDeltaLen())

	// Test key changes with queue
	sub.keyHasChanged([]interface{}{1}, false) // Insert/Replace
	assert.Equal(t, 1, sub.getDeltaLen())

	sub.keyHasChanged([]interface{}{2}, true) // Delete
	assert.Equal(t, 2, sub.getDeltaLen())

	// Verify queue order is maintained
	sub.Lock()
	assert.Len(t, sub.deltaQueue, 2)
	assert.False(t, sub.deltaQueue[0].isDelete)
	assert.True(t, sub.deltaQueue[1].isDelete)
	sub.Unlock()
}

func TestKeyAboveWatermark(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &subscription{
		c:          client,
		table:      srcTable,
		newTable:   dstTable,
		deltaMap:   make(map[string]bool),
		deltaQueue: nil,
	}

	// Test with watermark optimization disabled
	sub.keyHasChanged([]interface{}{1}, false)
	assert.Equal(t, 1, sub.getDeltaLen())

	// Setup watermark callback
	watermark := 5
	sub.keyAboveCopierCallback = func(key interface{}) bool {
		return key.(int) > watermark
	}

	// Enable watermark optimization
	sub.setKeyAboveWatermarkOptimization(true)

	// Test key below watermark
	sub.keyHasChanged([]interface{}{3}, false)
	assert.Equal(t, 2, sub.getDeltaLen())

	// Test key above watermark
	sub.keyHasChanged([]interface{}{10}, false)
	assert.Equal(t, 2, sub.getDeltaLen()) // Should not increase as key is above watermark
}

func TestFlushWithLock(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	client := &Client{
		db:              db,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &subscription{
		c:          client,
		table:      srcTable,
		newTable:   dstTable,
		deltaMap:   make(map[string]bool),
		deltaQueue: nil,
	}

	// Insert test data
	testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES (1, 'test1'), (2, 'test2')`)

	// Add some changes
	sub.keyHasChanged([]interface{}{1}, false)
	sub.keyHasChanged([]interface{}{2}, true)

	// Create a table lock
	lock, err := dbconn.NewTableLock(t.Context(), db, srcTable, dbconn.NewDBConfig(), logrus.New())
	assert.NoError(t, err)

	// Test flush with lock
	err = sub.flush(t.Context(), true, lock)
	assert.NoError(t, err)

	lock.Close()

	// Verify the changes were applied
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count) // Only ID 1 should be present, ID 2 was deleted
}

func TestFlushWithoutLock(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	client := &Client{
		db:              db,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 2,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &subscription{
		c:          client,
		table:      srcTable,
		newTable:   dstTable,
		deltaMap:   make(map[string]bool),
		deltaQueue: nil,
	}

	// Insert test data
	testutils.RunSQL(t, `INSERT INTO subscription_test (id, name) VALUES 
		(1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4')`)

	// Add multiple changes to test batch processing
	sub.keyHasChanged([]interface{}{1}, false)
	sub.keyHasChanged([]interface{}{2}, false)
	sub.keyHasChanged([]interface{}{3}, true)
	sub.keyHasChanged([]interface{}{4}, true)

	// Test flush without lock
	err = sub.flush(t.Context(), false, nil)
	assert.NoError(t, err)

	// Verify the changes were applied
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM _subscription_test_new").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count) // IDs 1 and 2 should be present, 3 and 4 were deleted
}

func TestConcurrentKeyChanges(t *testing.T) {
	srcTable, dstTable := setupTestTables(t)

	client := &Client{
		db:              nil,
		logger:          logrus.New(),
		concurrency:     2,
		targetBatchSize: 1000,
		dbConfig:        dbconn.NewDBConfig(),
	}

	sub := &subscription{
		c:          client,
		table:      srcTable,
		newTable:   dstTable,
		deltaMap:   make(map[string]bool),
		deltaQueue: nil,
	}

	// Run concurrent key changes
	done := make(chan bool)
	go func() {
		for i := range 100 {
			sub.keyHasChanged([]interface{}{i}, false)
		}
		done <- true
	}()

	go func() {
		for i := 100; i < 200; i++ {
			sub.keyHasChanged([]interface{}{i}, true)
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done

	assert.Equal(t, 200, sub.getDeltaLen())
}

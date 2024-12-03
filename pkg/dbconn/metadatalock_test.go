package dbconn

import (
	"context"
	"testing"
	"time"

	"github.com/cashapp/spirit/pkg/table"

	"github.com/cashapp/spirit/pkg/testutils"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestMetadataLock(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "test"}
	logger := logrus.New()
	mdl, err := NewMetadataLock(context.Background(), testutils.DSN(), &lockTableInfo, logger)
	assert.NoError(t, err)
	assert.NotNil(t, mdl)

	// Confirm a second lock cannot be acquired
	_, err = NewMetadataLock(context.Background(), testutils.DSN(), &lockTableInfo, logger)
	assert.ErrorContains(t, err, "lock is held by another connection")

	// Close the original mdl
	assert.NoError(t, mdl.Close())

	// Confirm a new lock can be acquired
	mdl3, err := NewMetadataLock(context.Background(), testutils.DSN(), &lockTableInfo, logger)
	assert.NoError(t, err)
	assert.NoError(t, mdl3.Close())
}

func TestMetadataLockContextCancel(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "test-cancel"}

	logger := logrus.New()
	ctx, cancel := context.WithCancel(context.Background())
	mdl, err := NewMetadataLock(ctx, testutils.DSN(), &lockTableInfo, logger)
	assert.NoError(t, err)
	assert.NotNil(t, mdl)

	// Cancel the context
	cancel()

	// Wait for the lock to be released
	<-mdl.closeCh

	// Confirm the lock is released by acquiring a new one
	mdl2, err := NewMetadataLock(context.Background(), testutils.DSN(), &lockTableInfo, logger)
	assert.NoError(t, err)
	assert.NotNil(t, mdl2)
	assert.NoError(t, mdl2.Close())
}

func TestMetadataLockRefresh(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "test-refresh"}
	logger := logrus.New()
	mdl, err := NewMetadataLock(context.Background(), testutils.DSN(), &lockTableInfo, logger, func(mdl *MetadataLock) {
		// override the refresh interval for faster testing
		mdl.refreshInterval = 2 * time.Second
	})
	assert.NoError(t, err)
	assert.NotNil(t, mdl)

	// wait for the refresh to happen
	time.Sleep(5 * time.Second)

	// Confirm the lock is still held
	_, err = NewMetadataLock(context.Background(), testutils.DSN(), &lockTableInfo, logger)
	assert.ErrorContains(t, err, "lock is held by another connection")

	// Close the lock
	assert.NoError(t, mdl.Close())
}

func TestMetadataLockLength(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "thisisareallylongtablenamethisisareallylongtablenamethisisareallylongtablename"}
	var empty *table.TableInfo

	logger := logrus.New()

	_, err := NewMetadataLock(context.Background(), testutils.DSN(), &lockTableInfo, logger)
	// No error anymore after using a hash of the table name
	assert.NoError(t, err)

	_, err = NewMetadataLock(context.Background(), testutils.DSN(), empty, logger)
	assert.ErrorContains(t, err, "metadata lock table info is nil")
}

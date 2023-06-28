package throttler

import (
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestThrottlerInterface(t *testing.T) {
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping test because REPLICA_DSN not set")
	}
	db, err := sql.Open("mysql", replicaDSN)
	assert.NoError(t, err)

	//	NewReplicationThrottler will attach either MySQL 8.0 or MySQL 5.7 throttler
	loopInterval = 1 * time.Millisecond
	throttler, err := NewReplicationThrottler(db, 60*time.Second, logrus.New())
	assert.NoError(t, err)
	assert.NoError(t, throttler.Open())

	time.Sleep(50 * time.Millisecond)        // make sure the throttler loop can calculate.
	throttler.BlockWait()                    // wait for catch up (there's no activity)
	assert.False(t, throttler.IsThrottled()) // there's a race, but its unlikely to be throttled

	assert.NoError(t, throttler.Close())

	time.Sleep(50 * time.Millisecond) // give it time to shutdown.
}

func TestMySQL57Throttler(t *testing.T) {
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping test because REPLICA_DSN not set")
	}
	db, err := sql.Open("mysql", replicaDSN)
	assert.NoError(t, err)

	// the MySQL 5.7 throttler should work with MySQL 8.0
	// So we can always test this.
	loopInterval = 50 * time.Millisecond
	throttler := &MySQL57Replica{
		Repl: Repl{
			replica:      db,
			lagTolerance: 120 * time.Second,
			logger:       logrus.New(),
		},
	}
	assert.NoError(t, throttler.Open())
	assert.False(t, throttler.IsThrottled())
	throttler.currentLagInMs = 100000
	assert.True(t, throttler.IsThrottled())

	// BlockWait until it catches up, but it should expire
	// because the 1s loop is set to 1ns
	blockWaitInterval = 1 * time.Nanosecond
	throttler.BlockWait() // prints to log, but allows some progress.

	// Wait >50ms and the loop should update the lag
	// to the correct value.
	time.Sleep(100 * time.Millisecond)
	assert.False(t, throttler.IsThrottled())
	assert.NoError(t, throttler.Close())
	// give it time to cleanup
	time.Sleep(100 * time.Millisecond)
}

func TestNoopThrottler(t *testing.T) {
	throttler := &Noop{}
	assert.NoError(t, throttler.Open())
	throttler.currentLag = 1 * time.Second
	throttler.lagTolerance = 2 * time.Second
	assert.False(t, throttler.IsThrottled())
	assert.NoError(t, throttler.UpdateLag())
	throttler.BlockWait()
	throttler.lagTolerance = 100 * time.Millisecond
	assert.True(t, throttler.IsThrottled())
	assert.NoError(t, throttler.Close())
}

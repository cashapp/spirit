package row

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCopierETAHistory(t *testing.T) {
	// Create a new CopierETAHistory
	history := newcopierEtaHistory()

	// Add an ETA
	history.addCurrentEstimateAndCompare(1 * time.Hour)
	assert.Equal(t, 1, len(history.etaHistory))
	assert.Equal(t, "", history.getComparison())

	// Add another ETA and confirm it is NOT stored because it is too recent
	history.addCurrentEstimateAndCompare(55 * time.Minute)
	assert.Equal(t, 1, len(history.etaHistory))

	// Even though the ETA was not stored, the latest estimate should still be updated
	assert.Equal(t, "5m from 0s ago", history.getComparison())

	// Create new CopierETAHistory with a history of 3 ETAs
	history = newcopierEtaHistory()
	history.addETA(copierETA{estimate: 3 * time.Hour, asOf: time.Now().Add(-2 * time.Hour)})
	history.addETA(copierETA{estimate: 2 * time.Hour, asOf: time.Now().Add(-1 * time.Hour)})
	history.addETA(copierETA{estimate: 1 * time.Hour, asOf: time.Now()})
	assert.Equal(t, 3, len(history.etaHistory))
	assert.Equal(t, "±0m from 2h ago", history.getComparison())

	// Create a new CopierETAHistory with a history of 24 ETAs
	history = newcopierEtaHistory()
	for i := 24; i > 0; i-- {
		history.addETA(copierETA{
			estimate: time.Duration(i) * time.Hour,
			asOf:     time.Now().Add(-time.Duration(i) * time.Hour),
		})
	}
	// Oldest history was auto-removed
	assert.Equal(t, 23, len(history.etaHistory))
	assert.Equal(t, "±0m from 22h ago", history.getComparison())

	comparison := history.addCurrentEstimateAndCompare(30 * time.Minute)
	assert.Equal(t, 24, len(history.etaHistory))
	assert.Equal(t, "-30m from 23h ago", comparison)
}

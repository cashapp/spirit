package migration

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMigrationStateString(t *testing.T) {
	assert.Equal(t, "initial", stateInitial.String())
	assert.Equal(t, "copyRows", stateCopyRows.String())
	assert.Equal(t, "waitingOnSentinelTable", stateWaitingOnSentinelTable.String())
	assert.Equal(t, "applyChangeset", stateApplyChangeset.String())
	assert.Equal(t, "checksum", stateChecksum.String())
	assert.Equal(t, "cutOver", stateCutOver.String())
	assert.Equal(t, "errCleanup", stateErrCleanup.String())
	assert.Equal(t, "analyzeTable", stateAnalyzeTable.String())
	assert.Equal(t, "close", stateClose.String())
}

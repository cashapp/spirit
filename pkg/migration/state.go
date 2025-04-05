package migration

type migrationState int32

const (
	stateInitial migrationState = iota
	stateCopyRows
	stateWaitingOnSentinelTable
	stateApplyChangeset // first mass apply
	stateAnalyzeTable
	stateChecksum
	statePostChecksum // second mass apply
	stateCutOver
	stateClose
	stateErrCleanup
)

func (s migrationState) String() string {
	switch s {
	case stateInitial:
		return "initial"
	case stateCopyRows:
		return "copyRows"
	case stateWaitingOnSentinelTable:
		return "waitingOnSentinelTable"
	case stateApplyChangeset:
		return "applyChangeset"
	case stateAnalyzeTable:
		return "analyzeTable"
	case stateChecksum:
		return "checksum"
	case statePostChecksum:
		return "postChecksum"
	case stateCutOver:
		return "cutOver"
	case stateClose:
		return "close"
	case stateErrCleanup:
		return "errCleanup"
	}
	return "unknown"
}

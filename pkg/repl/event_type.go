package repl

import "github.com/go-mysql-org/go-mysql/replication"

type eventType int

const (
	eventTypeUnknown eventType = iota
	eventTypeDelete
	eventTypeInsert
	eventTypeUpdate
)

// parseEventType converts a replication.EventType to a custom eventType.
// This follows the logic of canal/sync.go:
// https://github.com/go-mysql-org/go-mysql/blob/ee9447d96b48783abb05ab76a12501e5f1161e47/canal/sync.go#L282-L294
// Except we no longer need to use canal directly.
func parseEventType(eventType replication.EventType) eventType {
	switch eventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2, replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
		return eventTypeInsert
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2, replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
		return eventTypeDelete
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2, replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
		return eventTypeUpdate
	default:
		return eventTypeUnknown
	}
}

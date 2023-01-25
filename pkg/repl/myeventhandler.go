package repl

import (
	"sync/atomic"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/squareup/gap-core/log"
)

type MyEventHandler struct {
	canal.DummyEventHandler
	client *Client
}

// OnRow is called when a row is discovered via replication.
// The event is of type e.Action and contains one
// or more rows in e.Rows. We find the PRIMARY KEY of the row:
// 1) If it exceeds the known high watermark of the chunker we throw it away.
// (we've not copied that data yet - it will be already up to date when we copy it later).
// 2) If it could have been copied already, we add it to the changeset.
// We only need to add the PK + if the operation was a delete.
// This will be used after copy rows to apply any changes that have been made.
func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	for _, row := range e.Rows {
		key := h.client.table.ExtractPrimaryKeyFromRowImage(row)
		atomic.AddInt64(&h.client.changesetRowsEventCount, 1)
		// Important! We can only apply this optimization while in migrationStateCopyRows.
		// If we do it too early, we might miss updates in-between starting the subscription,
		// and opening the table in resume from checkpoint etc.
		if h.client.table.Chunker != nil && !h.client.disableKeyAboveWatermarkOptimization && h.client.table.Chunker.KeyAboveHighWatermark(key[0]) {
			continue // key can be ignored
		}
		switch e.Action {
		// TODO: check if the modification to the table was a DDL
		// If there were any other DDLs, we need to abandon this.
		// It's not safe to continue.
		case canal.InsertAction, canal.UpdateAction:
			h.client.keyHasChanged(key, false)
		case canal.DeleteAction:
			h.client.keyHasChanged(key, true)
		default:
			h.client.logger.WithFields(log.Fields{
				"action": e.Action,
			}).Error("unknown action")
		}
	}
	return nil
}

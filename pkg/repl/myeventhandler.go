package repl

import (
	"sync/atomic"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/replication"
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
		case canal.InsertAction, canal.UpdateAction:
			h.client.keyHasChanged(key, false)
		case canal.DeleteAction:
			h.client.keyHasChanged(key, true)
		default:
			h.client.logger.Errorf("unknown action: %v", e.Action)
		}
	}
	h.client.updatePosInMemory(e.Header.LogPos)
	return nil
}

// OnRotate is called when a rotate event is discovered via replication.
// We use this to capture the log file name, since only the position is caught on the row event.
func (h *MyEventHandler) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	h.client.Lock()
	defer h.client.Unlock()
	h.client.lastLogFileName = string(rotateEvent.NextLogName)
	return nil
}

// OnTableChanged is called when a table is changed via DDL.
// This is a failsafe because we don't expect DDL to be performed on the table while we are operating.
func (h *MyEventHandler) OnTableChanged(header *replication.EventHeader, schema string, table string) error {
	if (h.client.table.SchemaName == schema && h.client.table.TableName == table) ||
		(h.client.shadowTable.SchemaName == schema && h.client.shadowTable.TableName == table) {
		h.client.tableChanged()
	}
	return nil
}

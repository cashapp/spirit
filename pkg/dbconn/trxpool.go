package dbconn

import (
	"context"
	"database/sql"
	"sync"
)

// Maybe there is a better way to do this. For the CHECKSUM algorithm we need
// not a set of DB connections, but a set of transactions which have all
// had a read-view created at a certain point in time. So we pre-create
// them in newTrxPool() under a mutex, and then have a simple Get() and Put()
// which is used by worker threads.

type TrxPool struct {
	sync.Mutex
	trxs []*sql.Tx
}

// NewTrxPool creates a pool of transactions which have already
// had their read-view created in REPEATABLE READ isolation.
func NewTrxPool(ctx context.Context, db *sql.DB, count int) (*TrxPool, error) {
	checksumTxns := make([]*sql.Tx, 0, count)
	for i := 0; i < count; i++ {
		trx, _ := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		_, err := trx.Exec("START TRANSACTION WITH CONSISTENT SNAPSHOT")
		if err != nil {
			return nil, err
		}
		// Set SQL mode, charset, etc.
		if err := StandardizeTrx(trx); err != nil {
			return nil, err
		}
		checksumTxns = append(checksumTxns, trx)
	}
	return &TrxPool{trxs: checksumTxns}, nil
}

// Get gets a transaction from the pool.
func (p *TrxPool) Get() *sql.Tx {
	p.Lock()
	defer p.Unlock()
	trx := p.trxs[0]
	p.trxs = p.trxs[1:]
	return trx
}

// Put puts a transaction back in the pool.
func (p *TrxPool) Put(trx *sql.Tx) {
	p.Lock()
	defer p.Unlock()
	p.trxs = append(p.trxs, trx)
}

// Close closes all transactions in the pool.
func (p *TrxPool) Close() error {
	for _, trx := range p.trxs {
		if err := trx.Rollback(); err != nil {
			return err
		}
	}
	return nil
}

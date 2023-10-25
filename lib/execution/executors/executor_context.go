// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package executors

import (
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
)

/**
 * ExecutorContext stores all the context necessary to run an executor.
 */
type ExecutorContext struct {
	catalog *catalog.Catalog
	bpm     *buffer.BufferPoolManager
	txn     *access.Transaction
}

func NewExecutorContext(catalog *catalog.Catalog, bpm *buffer.BufferPoolManager, txn *access.Transaction) *ExecutorContext {
	return &ExecutorContext{catalog, bpm, txn}
}

func (e *ExecutorContext) GetCatalog() *catalog.Catalog {
	return e.catalog
}

func (e *ExecutorContext) GetBufferPoolManager() *buffer.BufferPoolManager {
	return e.bpm
}

func (e *ExecutorContext) GetTransaction() *access.Transaction {
	return e.txn
}

func (e *ExecutorContext) SetTransaction(txn *access.Transaction) {
	e.txn = txn
}

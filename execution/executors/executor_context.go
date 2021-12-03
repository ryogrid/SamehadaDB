// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package executors

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
)

// ExecutorContext stores all the context necessary to run an executor
type ExecutorContext struct {
	catalog *catalog.Catalog
	bpm     *buffer.BufferPoolManager
}

func NewExecutorContext(catalog *catalog.Catalog, bpm *buffer.BufferPoolManager) *ExecutorContext {
	return &ExecutorContext{catalog, bpm}
}

func (e *ExecutorContext) GetCatalog() *catalog.Catalog {
	return e.catalog
}

func (e *ExecutorContext) GetBufferPoolManager() *buffer.BufferPoolManager {
	return e.bpm
}

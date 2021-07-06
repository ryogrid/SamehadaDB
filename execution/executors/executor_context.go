package executors

import (
	"github.com/brunocalza/go-bustub/storage/buffer"
	"github.com/brunocalza/go-bustub/storage/table"
)

// ExecutorContext stores all the context necessary to run an executor
type ExecutorContext struct {
	catalog *table.Catalog
	bpm     *buffer.BufferPoolManager
}

func NewExecutorContext(catalog *table.Catalog, bpm *buffer.BufferPoolManager) *ExecutorContext {
	return &ExecutorContext{catalog, bpm}
}

func (e *ExecutorContext) GetCatalog() *table.Catalog {
	return e.catalog
}

func (e *ExecutorContext) GetBufferPoolManager() *buffer.BufferPoolManager {
	return e.bpm
}

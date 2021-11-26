package executors

import (
	"github.com/ryogrid/SaitomDB/catalog"
	"github.com/ryogrid/SaitomDB/storage/buffer"
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

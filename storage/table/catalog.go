package table

import (
	"github.com/brunocalza/go-bustub/storage/buffer"
)

// Catalog is a non-persistent catalog that is designed for the executor to use.
// It handles table creation and table lookup
type Catalog struct {
	bpm         *buffer.BufferPoolManager
	tables      map[uint32]*TableMetadata
	names       map[string]uint32
	nextTableId uint32
}

func NewCatalog(bpm *buffer.BufferPoolManager) *Catalog {
	return &Catalog{bpm, make(map[uint32]*TableMetadata), make(map[string]uint32), 0}
}

func (c *Catalog) GetTableByName(table string) *TableMetadata {
	oid := c.names[table]
	return c.tables[oid]
}

func (c *Catalog) GetTableByOID(oid uint32) *TableMetadata {
	return c.tables[oid]
}

func (c *Catalog) CreateTable(name string, schema *Schema) *TableMetadata {
	oid := c.nextTableId
	c.nextTableId++
	c.names[name] = oid

	tableHeap := NewTableHeap(c.bpm)
	tableMetadata := &TableMetadata{schema, name, tableHeap, oid}
	c.tables[oid] = tableMetadata

	return tableMetadata
}

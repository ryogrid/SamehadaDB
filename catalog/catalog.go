package catalog

import (
	"github.com/brunocalza/go-bustub/storage/access"
	"github.com/brunocalza/go-bustub/storage/buffer"
	"github.com/brunocalza/go-bustub/storage/table"
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
	if oid, ok := c.names[table]; !ok {
		return c.tables[oid]
	}
	return nil
}

func (c *Catalog) GetTableByOID(oid uint32) *TableMetadata {
	if table, ok := c.tables[oid]; ok {
		return table
	}
	return nil
}

// CreateTable creates a new table and return its metadata
func (c *Catalog) CreateTable(name string, schema *table.Schema) *TableMetadata {
	oid := c.nextTableId
	c.nextTableId++
	c.names[name] = oid

	tableHeap := access.NewTableHeap(c.bpm)
	tableMetadata := &TableMetadata{schema, name, tableHeap, oid}
	c.tables[oid] = tableMetadata

	//c.InsertTable()

	return tableMetadata
}

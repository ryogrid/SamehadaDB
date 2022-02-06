// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package catalog

import (
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

// TableCatalogPageId indicates the page where the table catalog can be found
// The first page is reserved for the table catalog
const TableCatalogPageId = 0

// ColumnsCatalogPageId indicates the page where the columns catalog can be found
// The second page is reserved for the table catalog
const ColumnsCatalogPageId = 1

const ColumnsCatalogOID = 0

// Catalog is a non-persistent catalog that is designed for the executor to use.
// It handles table creation and table lookup
type Catalog struct {
	bpm          *buffer.BufferPoolManager
	tableIds     map[uint32]*TableMetadata
	tableNames   map[string]*TableMetadata
	nextTableId  uint32
	tableHeap    *access.TableHeap
	Log_manager  *recovery.LogManager
	Lock_manager *access.LockManager
}

// BootstrapCatalog bootstrap the systems' catalogs on the first database initialization
func BootstrapCatalog(bpm *buffer.BufferPoolManager, log_manager *recovery.LogManager, lock_manager *access.LockManager, txn *access.Transaction) *Catalog {
	tableCatalogHeap := access.NewTableHeap(bpm, log_manager, lock_manager, txn)
	tableCatalog := &Catalog{bpm, make(map[uint32]*TableMetadata), make(map[string]*TableMetadata), 0, tableCatalogHeap, log_manager, lock_manager}
	tableCatalog.CreateTable("columns_catalog", ColumnsCatalogSchema(), txn)
	return tableCatalog
}

// GetCatalog get all information about tables and columns from disk and put it on memory
func GetCatalog(bpm *buffer.BufferPoolManager, log_manager *recovery.LogManager, lock_manager *access.LockManager, txn *access.Transaction) *Catalog {
	tableCatalogHeapIt := access.InitTableHeap(bpm, TableCatalogPageId, log_manager, lock_manager).Iterator(txn)

	tableIds := make(map[uint32]*TableMetadata)
	tableNames := make(map[string]*TableMetadata)

	for tuple := tableCatalogHeapIt.Current(); !tableCatalogHeapIt.End(); tuple = tableCatalogHeapIt.Next() {
		oid := tuple.GetValue(TableCatalogSchema(), TableCatalogSchema().GetColIndex("oid")).ToInteger()
		name := tuple.GetValue(TableCatalogSchema(), TableCatalogSchema().GetColIndex("name")).ToVarchar()
		firstPage := tuple.GetValue(TableCatalogSchema(), TableCatalogSchema().GetColIndex("first_page")).ToInteger()

		columns := []*column.Column{}
		columnsCatalogHeapIt := access.InitTableHeap(bpm, ColumnsCatalogPageId, log_manager, lock_manager).Iterator(txn)
		for tuple := columnsCatalogHeapIt.Current(); !columnsCatalogHeapIt.End(); tuple = columnsCatalogHeapIt.Next() {
			tableOid := tuple.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("table_oid")).ToInteger()
			if tableOid != oid {
				continue
			}
			columnType := tuple.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("type")).ToInteger()
			columnName := tuple.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("name")).ToVarchar()
			//fixedLength := tuple.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("fixed_length")).ToInteger()
			//variableLength := tuple.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("variable_length")).ToInteger()
			//columnOffset := tuple.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("offset")).ToInteger()

			columns = append(columns, column.NewColumn(columnName, types.TypeID(columnType)))
		}

		tableMetadata := &TableMetadata{
			schema.NewSchema(columns),
			name,
			access.InitTableHeap(bpm, types.PageID(firstPage), log_manager, lock_manager),
			uint32(oid)}

		tableIds[uint32(oid)] = tableMetadata
		tableNames[name] = tableMetadata
	}

	return &Catalog{bpm, tableIds, tableNames, 1, access.InitTableHeap(bpm, 0, log_manager, lock_manager), log_manager, lock_manager}

}

func (c *Catalog) GetTableByName(table string) *TableMetadata {
	if table, ok := c.tableNames[table]; ok {
		return table
	}
	return nil
}

func (c *Catalog) GetTableByOID(oid uint32) *TableMetadata {
	if table, ok := c.tableIds[oid]; ok {
		return table
	}
	return nil
}

// CreateTable creates a new table and return its metadata
func (c *Catalog) CreateTable(name string, schema *schema.Schema, txn *access.Transaction) *TableMetadata {
	oid := c.nextTableId
	c.nextTableId++

	tableHeap := access.NewTableHeap(c.bpm, c.Log_manager, c.Lock_manager, txn)
	tableMetadata := &TableMetadata{schema, name, tableHeap, oid}

	c.tableIds[oid] = tableMetadata
	c.tableNames[name] = tableMetadata
	// TODO: (SDB) this InsertTable call is needed?
	c.InsertTable(tableMetadata, txn)

	return tableMetadata
}

func (c *Catalog) InsertTable(tableMetadata *TableMetadata, txn *access.Transaction) {
	row := make([]types.Value, 0)

	row = append(row, types.NewInteger(int32(tableMetadata.oid)))
	row = append(row, types.NewVarchar(tableMetadata.name))
	row = append(row, types.NewInteger(int32(tableMetadata.table.GetFirstPageId())))
	first_tuple := tuple.NewTupleFromSchema(row, TableCatalogSchema())

	c.tableHeap.InsertTuple(first_tuple, txn)
	for _, column := range tableMetadata.schema.GetColumns() {
		row := make([]types.Value, 0)
		row = append(row, types.NewInteger(int32(tableMetadata.oid)))
		row = append(row, types.NewInteger(int32(column.GetType())))
		row = append(row, types.NewVarchar(column.GetColumnName()))
		row = append(row, types.NewInteger(int32(column.FixedLength())))
		row = append(row, types.NewInteger(int32(column.VariableLength())))
		row = append(row, types.NewInteger(int32(column.GetOffset())))
		new_tuple := tuple.NewTupleFromSchema(row, ColumnsCatalogSchema())

		c.tableIds[ColumnsCatalogOID].Table().InsertTuple(new_tuple, txn)
	}
}

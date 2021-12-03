// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package catalog

import (
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/table"
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
	bpm         *buffer.BufferPoolManager
	tableIds    map[uint32]*TableMetadata
	tableNames  map[string]*TableMetadata
	nextTableId uint32
	*access.TableHeap
}

// BootstrapCatalog bootstrap the systems' catalogs on the first database initialization
func BootstrapCatalog(bpm *buffer.BufferPoolManager) *Catalog {
	tableCatalogHeap := access.NewTableHeap(bpm)
	tableCatalog := &Catalog{bpm, make(map[uint32]*TableMetadata), make(map[string]*TableMetadata), 0, tableCatalogHeap}
	tableCatalog.CreateTable("columns_catalog", ColumnsCatalogSchema())
	return tableCatalog
}

// GetCatalog get all information about tables and columns from disk and put it on memory
func GetCatalog(bpm *buffer.BufferPoolManager) *Catalog {
	tableCatalogHeapIt := access.InitTableHeap(bpm, TableCatalogPageId).Iterator()

	tableIds := make(map[uint32]*TableMetadata)
	tableNames := make(map[string]*TableMetadata)

	for tuple := tableCatalogHeapIt.Current(); !tableCatalogHeapIt.End(); tuple = tableCatalogHeapIt.Next() {
		oid := tuple.GetValue(TableCatalogSchema(), TableCatalogSchema().GetColIndex("oid")).ToInteger()
		name := tuple.GetValue(TableCatalogSchema(), TableCatalogSchema().GetColIndex("name")).ToVarchar()
		firstPage := tuple.GetValue(TableCatalogSchema(), TableCatalogSchema().GetColIndex("first_page")).ToInteger()

		columns := []*table.Column{}
		columnsCatalogHeapIt := access.InitTableHeap(bpm, ColumnsCatalogPageId).Iterator()
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

			columns = append(columns, table.NewColumn(columnName, types.TypeID(columnType)))
		}

		tableMetadata := &TableMetadata{
			table.NewSchema(columns),
			name,
			access.InitTableHeap(bpm, types.PageID(firstPage)),
			uint32(oid)}

		tableIds[uint32(oid)] = tableMetadata
		tableNames[name] = tableMetadata
	}

	return &Catalog{bpm, tableIds, tableNames, 1, access.InitTableHeap(bpm, 0)}

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
func (c *Catalog) CreateTable(name string, schema *table.Schema) *TableMetadata {
	oid := c.nextTableId
	c.nextTableId++

	tableHeap := access.NewTableHeap(c.bpm)
	tableMetadata := &TableMetadata{schema, name, tableHeap, oid}

	c.tableIds[oid] = tableMetadata
	c.tableNames[name] = tableMetadata
	c.InsertTable(tableMetadata)

	return tableMetadata
}

func (c *Catalog) InsertTable(tableMetadata *TableMetadata) {
	row := make([]types.Value, 0)

	row = append(row, types.NewInteger(int32(tableMetadata.oid)))
	row = append(row, types.NewVarchar(tableMetadata.name))
	row = append(row, types.NewInteger(int32(tableMetadata.table.GetFirstPageId())))
	tuple := table.NewTupleFromSchema(row, TableCatalogSchema())

	c.TableHeap.InsertTuple(tuple)
	for _, column := range tableMetadata.schema.GetColumns() {
		row := make([]types.Value, 0)
		row = append(row, types.NewInteger(int32(tableMetadata.oid)))
		row = append(row, types.NewInteger(int32(column.GetType())))
		row = append(row, types.NewVarchar(column.GetColumnName()))
		row = append(row, types.NewInteger(int32(column.FixedLength())))
		row = append(row, types.NewInteger(int32(column.VariableLength())))
		row = append(row, types.NewInteger(int32(column.GetOffset())))
		tuple := table.NewTupleFromSchema(row, ColumnsCatalogSchema())

		c.tableIds[ColumnsCatalogOID].Table().InsertTuple(tuple)
	}
}

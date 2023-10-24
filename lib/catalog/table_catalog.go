// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package catalog

import (
	"strings"
	"sync"

	"github.com/ryogrid/SamehadaDB/lib/storage/index"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	"sync/atomic"

	"github.com/ryogrid/SamehadaDB/lib/recovery"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/column"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
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
	bpm        *buffer.BufferPoolManager
	tableIds   map[uint32]*TableMetadata
	tableNames map[string]*TableMetadata
	// incrementation must be atomic
	nextTableId     uint32
	tableHeap       *access.TableHeap
	Log_manager     *recovery.LogManager
	Lock_manager    *access.LockManager
	tableIdsMutex   *sync.Mutex
	tableNamesMutex *sync.Mutex
}

func Int32toBool(val int32) bool {
	if val == 1 {
		return true
	} else {
		return false
	}
}

// BootstrapCatalog bootstrap the systems' catalogs on the first database initialization
func BootstrapCatalog(bpm *buffer.BufferPoolManager, log_manager *recovery.LogManager, lock_manager *access.LockManager, txn *access.Transaction) *Catalog {
	tableCatalogHeap := access.NewTableHeap(bpm, log_manager, lock_manager, txn)
	tableCatalog := &Catalog{bpm, make(map[uint32]*TableMetadata), make(map[string]*TableMetadata), 0, tableCatalogHeap, log_manager, lock_manager, new(sync.Mutex), new(sync.Mutex)}
	tableCatalog.CreateTable("columns_catalog", ColumnsCatalogSchema(), txn)
	return tableCatalog
}

// RecoveryCatalogFromCatalogPage get all information about tables and columns from disk and put it on memory
func RecoveryCatalogFromCatalogPage(bpm *buffer.BufferPoolManager, log_manager *recovery.LogManager, lock_manager *access.LockManager, txn *access.Transaction) *Catalog {
	tableCatalogHeapIt := access.InitTableHeap(bpm, TableCatalogPageId, log_manager, lock_manager).Iterator(txn)

	tableIds := make(map[uint32]*TableMetadata)
	tableNames := make(map[string]*TableMetadata)

	for tuple_outer := tableCatalogHeapIt.Current(); !tableCatalogHeapIt.End(); tuple_outer = tableCatalogHeapIt.Next() {
		oid := tuple_outer.GetValue(TableCatalogSchema(), TableCatalogSchema().GetColIndex("oid")).ToInteger()
		name := tuple_outer.GetValue(TableCatalogSchema(), TableCatalogSchema().GetColIndex("name")).ToVarchar()
		firstPage := tuple_outer.GetValue(TableCatalogSchema(), TableCatalogSchema().GetColIndex("first_page")).ToInteger()

		columns := []*column.Column{}
		columnsCatalogHeapIt := access.InitTableHeap(bpm, ColumnsCatalogPageId, log_manager, lock_manager).Iterator(txn)
		for tuple_inner := columnsCatalogHeapIt.Current(); !columnsCatalogHeapIt.End(); tuple_inner = columnsCatalogHeapIt.Next() {
			tableOid := tuple_inner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("table_oid")).ToInteger()
			if tableOid != oid {
				continue
			}
			columnType := tuple_inner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("type")).ToInteger()
			columnName := tuple_inner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("name")).ToVarchar()
			fixedLength := tuple_inner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("fixed_length")).ToInteger()
			variableLength := tuple_inner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("variable_length")).ToInteger()
			columnOffset := tuple_inner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("offset")).ToInteger()
			hasIndex := Int32toBool(tuple_inner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("has_index")).ToInteger())
			indexKind := tuple_inner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("index_kind")).ToInteger()
			indexHeaderPageId := tuple_inner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("index_header_page_id")).ToInteger()

			column_ := column.NewColumn(columnName, types.TypeID(columnType), false, index_constants.INDEX_KIND_INVALID, types.PageID(indexHeaderPageId), nil)
			column_.SetFixedLength(uint32(fixedLength))
			column_.SetVariableLength(uint32(variableLength))
			column_.SetOffset(uint32(columnOffset))
			column_.SetHasIndex(hasIndex)
			column_.SetIndexKind(index_constants.IndexKind(indexKind))
			column_.SetIndexHeaderPageId(types.PageID(indexHeaderPageId))

			columns = append(columns, column_)
		}

		tableMetadata := NewTableMetadata(
			schema.NewSchema(columns),
			name,
			access.InitTableHeap(bpm, types.PageID(firstPage), log_manager, lock_manager),
			uint32(oid))

		tableIds[uint32(oid)] = tableMetadata
		tableNames[name] = tableMetadata
	}

	return &Catalog{bpm, tableIds, tableNames, 1, access.InitTableHeap(bpm, 0, log_manager, lock_manager), log_manager, lock_manager, new(sync.Mutex), new(sync.Mutex)}
}

func (c *Catalog) GetTableByName(table string) *TableMetadata {
	// note: alphabets on table name is stored in lowercase
	tableName := strings.ToLower(table)
	c.tableNamesMutex.Lock()
	defer c.tableNamesMutex.Unlock()
	if table_, ok := c.tableNames[tableName]; ok {
		return table_
	}
	return nil
}

func (c *Catalog) GetTableByOID(oid uint32) *TableMetadata {
	c.tableIdsMutex.Lock()
	defer c.tableIdsMutex.Unlock()
	if table, ok := c.tableIds[oid]; ok {
		return table
	}
	return nil
}

func (c *Catalog) GetAllTables() []*TableMetadata {
	ret := make([]*TableMetadata, 0)
	c.tableIdsMutex.Lock()
	defer c.tableIdsMutex.Unlock()
	for key, _ := range c.tableIds {
		ret = append(ret, c.tableIds[key])
	}
	return ret
}

func attachTableNameToColumnsName(schema_ *schema.Schema, tblName string) *schema.Schema {
	for ii := 0; ii < int(schema_.GetColumnCount()); ii++ {
		col := schema_.GetColumn(uint32(ii))
		curName := col.GetColumnName()
		if strings.Contains(curName, ".") {
			continue
		}
		col.SetColumnName(tblName + "." + curName)
	}
	return schema_
}

// CreateTable creates a new table and return its metadata
// ATTENTION: this function modifies column name filed of Column objects on *schema_* argument if needed
func (c *Catalog) CreateTable(name string, schema_ *schema.Schema, txn *access.Transaction) *TableMetadata {
	// note: alphabets on table name is stored in lowercase
	name_ := strings.ToLower(name)

	oid := c.nextTableId
	atomic.AddUint32(&c.nextTableId, 1)

	tableHeap := access.NewTableHeap(c.bpm, c.Log_manager, c.Lock_manager, txn)

	// attach table name as prefix to all columns name
	attachTableNameToColumnsName(schema_, name_)

	tableMetadata := NewTableMetadata(schema_, name_, tableHeap, oid)

	c.tableIdsMutex.Lock()
	c.tableIds[oid] = tableMetadata
	c.tableIdsMutex.Unlock()
	c.tableNamesMutex.Lock()
	c.tableNames[name_] = tableMetadata
	c.tableNamesMutex.Unlock()
	c.insertTable(tableMetadata, txn)

	return tableMetadata
}

func boolToInt32(val bool) int32 {
	if val {
		return 1
	} else {
		return 0
	}
}

func (c *Catalog) insertTable(tableMetadata *TableMetadata, txn *access.Transaction) {
	row := make([]types.Value, 0)

	row = append(row, types.NewInteger(int32(tableMetadata.oid)))
	row = append(row, types.NewVarchar(tableMetadata.name))
	row = append(row, types.NewInteger(int32(tableMetadata.table.GetFirstPageId())))
	first_tuple := tuple.NewTupleFromSchema(row, TableCatalogSchema())

	// insert entry to TableCatalogPage (PageId = 0)
	c.tableHeap.InsertTuple(first_tuple, false, txn, tableMetadata.OID())
	for _, column_ := range tableMetadata.schema.GetColumns() {
		row := make([]types.Value, 0)
		row = append(row, types.NewInteger(int32(tableMetadata.oid)))
		row = append(row, types.NewInteger(int32(column_.GetType())))
		row = append(row, types.NewVarchar(column_.GetColumnName()))
		row = append(row, types.NewInteger(int32(column_.FixedLength())))
		row = append(row, types.NewInteger(int32(column_.VariableLength())))
		row = append(row, types.NewInteger(int32(column_.GetOffset())))
		row = append(row, types.NewInteger(boolToInt32(column_.HasIndex())))
		row = append(row, types.NewInteger(int32(column_.IndexKind())))
		row = append(row, types.NewInteger(int32(column_.IndexHeaderPageId())))
		new_tuple := tuple.NewTupleFromSchema(row, ColumnsCatalogSchema())

		// insert entry to ColumnsCatalogPage (PageId = 1)
		c.tableIds[ColumnsCatalogOID].Table().InsertTuple(new_tuple, false, txn, ColumnsCatalogOID)
	}
	// flush a page having table definitions
	c.bpm.FlushPage(TableCatalogPageId)
	// flush a page having columns definitions on table
	c.bpm.FlushPage(ColumnsCatalogPageId)
}

// for Redo/Undo
//
// returned list's length is same with column num of table.
// value of elements corresponding to columns which doesn't have index is nil.
func (c *Catalog) GetRollbackNeededIndexes(indexMap map[uint32][]index.Index, oid uint32) []index.Index {
	if indexes, found := indexMap[oid]; found {
		return indexes
	} else {
		indexes_ := c.GetTableByOID(oid).Indexes()
		indexMap[oid] = indexes_
		return indexes_
	}
}

func (c *Catalog) GetColValFromTupleForRollback(tuple_ *tuple.Tuple, colIdx uint32, oid uint32) *types.Value {
	schema_ := c.GetTableByOID(oid).Schema()
	val := tuple_.GetValue(schema_, colIdx)
	return &val
}

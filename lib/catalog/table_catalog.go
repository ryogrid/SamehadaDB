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

// TableCatalogPageID indicates the page where the table catalog can be found
// The first page is reserved for the table catalog
const TableCatalogPageID = 0

// ColumnsCatalogPageID indicates the page where the columns catalog can be found
// The second page is reserved for the table catalog
const ColumnsCatalogPageID = 1

const ColumnsCatalogOID = 0

// Catalog is a non-persistent catalog that is designed for the executor to use.
// It handles table creation and table lookup
type Catalog struct {
	bpm        *buffer.BufferPoolManager
	tableIDs   map[uint32]*TableMetadata
	tableNames map[string]*TableMetadata
	// incrementation must be atomic
	nextTableID     uint32
	tableHeap       *access.TableHeap
	LogManager      *recovery.LogManager
	LockManager     *access.LockManager
	tableIDsMutex   *sync.Mutex
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
func BootstrapCatalog(bpm *buffer.BufferPoolManager, logManager *recovery.LogManager, lockManager *access.LockManager, txn *access.Transaction) *Catalog {
	tableCatalogHeap := access.NewTableHeap(bpm, logManager, lockManager, txn)
	tableCatalog := &Catalog{bpm, make(map[uint32]*TableMetadata), make(map[string]*TableMetadata), 0, tableCatalogHeap, logManager, lockManager, new(sync.Mutex), new(sync.Mutex)}
	tableCatalog.CreateTable("columns_catalog", ColumnsCatalogSchema(), txn)
	return tableCatalog
}

// RecoveryCatalogFromCatalogPage get all information about tables and columns from disk and put it on memory
func RecoveryCatalogFromCatalogPage(bpm *buffer.BufferPoolManager, logManager *recovery.LogManager, lockManager *access.LockManager, txn *access.Transaction, isGracefulShutdown bool) *Catalog {
	tableCatalogHeapIt := access.InitTableHeap(bpm, TableCatalogPageID, logManager, lockManager).Iterator(txn)

	tableIDs := make(map[uint32]*TableMetadata)
	tableNames := make(map[string]*TableMetadata)

	for tupleOuter := tableCatalogHeapIt.Current(); !tableCatalogHeapIt.End(); tupleOuter = tableCatalogHeapIt.Next() {
		oid := tupleOuter.GetValue(TableCatalogSchema(), TableCatalogSchema().GetColIndex("oid")).ToInteger()
		name := tupleOuter.GetValue(TableCatalogSchema(), TableCatalogSchema().GetColIndex("name")).ToVarchar()
		firstPage := tupleOuter.GetValue(TableCatalogSchema(), TableCatalogSchema().GetColIndex("first_page")).ToInteger()

		columns := []*column.Column{}
		columnsCatalogHeapIt := access.InitTableHeap(bpm, ColumnsCatalogPageID, logManager, lockManager).Iterator(txn)
		for tupleInner := columnsCatalogHeapIt.Current(); !columnsCatalogHeapIt.End(); tupleInner = columnsCatalogHeapIt.Next() {
			tableOid := tupleInner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("table_oid")).ToInteger()
			if tableOid != oid {
				continue
			}
			columnType := tupleInner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("type")).ToInteger()
			columnName := tupleInner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("name")).ToVarchar()
			fixedLength := tupleInner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("fixed_length")).ToInteger()
			variableLength := tupleInner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("variable_length")).ToInteger()
			columnOffset := tupleInner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("offset")).ToInteger()
			hasIndex := Int32toBool(tupleInner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("has_index")).ToInteger())
			indexKind := tupleInner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("index_kind")).ToInteger()
			indexHeaderPageID := tupleInner.GetValue(ColumnsCatalogSchema(), ColumnsCatalogSchema().GetColIndex("index_header_page_id")).ToInteger()

			col := column.NewColumn(columnName, types.TypeID(columnType), false, index_constants.IndexKindInvalid, types.PageID(indexHeaderPageID), nil)
			col.SetFixedLength(uint32(fixedLength))
			col.SetVariableLength(uint32(variableLength))
			col.SetOffset(uint32(columnOffset))
			col.SetHasIndex(hasIndex)
			col.SetIndexKind(index_constants.IndexKind(indexKind))
			col.SetIndexHeaderPageID(types.PageID(indexHeaderPageID))

			columns = append(columns, col)
		}

		tableMetadata := NewTableMetadata(
			schema.NewSchema(columns),
			name,
			access.InitTableHeap(bpm, types.PageID(firstPage), logManager, lockManager),
			uint32(oid),
			logManager,
			isGracefulShutdown,
		)

		tableIDs[uint32(oid)] = tableMetadata
		tableNames[name] = tableMetadata
	}

	return &Catalog{bpm, tableIDs, tableNames, 1, access.InitTableHeap(bpm, 0, logManager, lockManager), logManager, lockManager, new(sync.Mutex), new(sync.Mutex)}
}

func (c *Catalog) GetTableByName(table string) *TableMetadata {
	// note: alphabets on table name is stored in lowercase
	tableName := strings.ToLower(table)
	c.tableNamesMutex.Lock()
	defer c.tableNamesMutex.Unlock()
	if tbl, ok := c.tableNames[tableName]; ok {
		return tbl
	}
	return nil
}

func (c *Catalog) GetTableByOID(oid uint32) *TableMetadata {
	c.tableIDsMutex.Lock()
	defer c.tableIDsMutex.Unlock()
	if table, ok := c.tableIDs[oid]; ok {
		return table
	}
	return nil
}

func (c *Catalog) GetAllTables() []*TableMetadata {
	ret := make([]*TableMetadata, 0)
	c.tableIDsMutex.Lock()
	defer c.tableIDsMutex.Unlock()
	for key := range c.tableIDs {
		ret = append(ret, c.tableIDs[key])
	}
	return ret
}

func attachTableNameToColumnsName(sc *schema.Schema, tblName string) *schema.Schema {
	for ii := 0; ii < int(sc.GetColumnCount()); ii++ {
		col := sc.GetColumn(uint32(ii))
		curName := col.GetColumnName()
		if strings.Contains(curName, ".") {
			continue
		}
		col.SetColumnName(tblName + "." + curName)
	}
	return sc
}

// CreateTable creates a new table and return its metadata
// ATTENTION: this function modifies column name filed of Column objects on *sc* argument if needed
func (c *Catalog) CreateTable(name string, sc *schema.Schema, txn *access.Transaction) *TableMetadata {
	// note: alphabets on table name is stored in lowercase
	lowerName := strings.ToLower(name)

	oid := c.nextTableID
	atomic.AddUint32(&c.nextTableID, 1)

	tableHeap := access.NewTableHeap(c.bpm, c.LogManager, c.LockManager, txn)

	// attach table name as prefix to all columns name
	attachTableNameToColumnsName(sc, lowerName)

	tableMetadata := NewTableMetadata(sc, lowerName, tableHeap, oid, c.LogManager, true)

	c.tableIDsMutex.Lock()
	c.tableIDs[oid] = tableMetadata
	c.tableIDsMutex.Unlock()
	c.tableNamesMutex.Lock()
	c.tableNames[lowerName] = tableMetadata
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
	row = append(row, types.NewInteger(int32(tableMetadata.table.GetFirstPageID())))
	firstTuple := tuple.NewTupleFromSchema(row, TableCatalogSchema())

	// insert entry to TableCatalogPage (PageID = 0)
	c.tableHeap.InsertTuple(firstTuple, txn, tableMetadata.OID(), false)
	for _, col := range tableMetadata.schema.GetColumns() {
		row := make([]types.Value, 0)
		row = append(row, types.NewInteger(int32(tableMetadata.oid)))
		row = append(row, types.NewInteger(int32(col.GetType())))
		row = append(row, types.NewVarchar(col.GetColumnName()))
		row = append(row, types.NewInteger(int32(col.FixedLength())))
		row = append(row, types.NewInteger(int32(col.VariableLength())))
		row = append(row, types.NewInteger(int32(col.GetOffset())))
		row = append(row, types.NewInteger(boolToInt32(col.HasIndex())))
		row = append(row, types.NewInteger(int32(col.IndexKind())))
		row = append(row, types.NewInteger(int32(col.IndexHeaderPageID())))
		newTuple := tuple.NewTupleFromSchema(row, ColumnsCatalogSchema())

		// insert entry to ColumnsCatalogPage (PageID = 1)
		c.tableIDs[ColumnsCatalogOID].Table().InsertTuple(newTuple, txn, ColumnsCatalogOID, false)
	}
	// flush a page having table definitions
	c.bpm.FlushPage(TableCatalogPageID)
	// flush a page having columns definitions on table
	c.bpm.FlushPage(ColumnsCatalogPageID)
}

// for Redo/Undo
//
// returned list's length is same with column num of table.
// value of elements corresponding to columns which doesn't have index is nil.
func (c *Catalog) GetRollbackNeededIndexes(indexMap map[uint32][]index.Index, oid uint32) []index.Index {
	if indexes, found := indexMap[oid]; found {
		return indexes
	} else {
		idxs := c.GetTableByOID(oid).Indexes()
		indexMap[oid] = idxs
		return idxs
	}
}

func (c *Catalog) GetColValFromTupleForRollback(tpl *tuple.Tuple, colIdx uint32, oid uint32) *types.Value {
	sc := c.GetTableByOID(oid).Schema()
	val := tpl.GetValue(sc, colIdx)
	return &val
}

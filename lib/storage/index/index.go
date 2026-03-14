package index

import (
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
)

/**
 * class IndexMetadata - Holds metadata of an index object
 *
 * The metadata object maintains the tuple schema and key attribute of an
 * index, since the external callers does not know the actual structure of
 * the index key, so it is the index's responsibility to maintain such a
 * mapping relation and does the conversion between tuple key and index key
 */

type IndexMetadata struct {
	name       string
	tableName  string
	// The mapping relation between key schema and tuple schema
	keyAttrs []uint32
	// schema of the indexed key
	tupleSchema *schema.Schema
}

func NewIndexMetadata(indexName string, tableName string, tupleSchema *schema.Schema,
	keyAttrs []uint32) *IndexMetadata {
	ret := new(IndexMetadata)
	ret.name = indexName
	ret.tableName = tableName
	ret.keyAttrs = keyAttrs
	ret.tupleSchema = tupleSchema
	return ret
}

func (im *IndexMetadata) GetName() *string      { return &im.name }
func (im *IndexMetadata) GetTableName() *string { return &im.tableName }

// Returns a schema object pointer that represents the indexed key
func (im *IndexMetadata) GetTupleSchema() *schema.Schema { return im.tupleSchema }

// Return the number of columns inside index key (not in tuple key)
// Note that this must be defined inside the cpp source file
// because it uses the member of catalog::Schema which is not known here
func (im *IndexMetadata) GetIndexColumnCount() uint32 { return uint32(len(im.keyAttrs)) }

// Returns the mapping relation between indexed columns  and base table
// columns
func (im *IndexMetadata) GetKeyAttrs() []uint32 { return im.keyAttrs }

/////////////////////////////////////////////////////////////////////
// Index class definition
/////////////////////////////////////////////////////////////////////

/**
 * class Index - Base class for derived indices of different types
 *
 * The index structure majorly maintains information on the schema of the
 * schema of the underlying table and the mapping relation between index key
 * and tuple key, and provides an abstracted way for the external world to
 * interact with the underlying index implementation without exposing
 * the actual implementation's interface.
 *
 * Index object also handles predicate scan, in addition to simple insert,
 * delete, predicate insert, point query, and full index scan. OnPredicate scan
 * only supports conjunction, and may or may not be optimized depending on
 * the type of expressions inside the predicate.
 */

type Index interface {
	// Return the metadata object associated with the index
	GetMetadata() *IndexMetadata
	GetIndexColumnCount() uint32
	GetName() *string
	GetTupleSchema() *schema.Schema
	GetKeyAttrs() []uint32
	///////////////////////////////////////////////////////////////////
	// Point Modification
	///////////////////////////////////////////////////////////////////
	// designed for secondary indexes.
	InsertEntry(*tuple.Tuple, page.RID, interface{})
	// delete the index entry linked to given tuple
	DeleteEntry(*tuple.Tuple, page.RID, interface{})
	// update entry. internally, delete first entry and insert seconde entry atomically
	UpdateEntry(*tuple.Tuple, page.RID, *tuple.Tuple, page.RID, interface{})
	ScanKey(*tuple.Tuple, interface{}) []page.RID
	// pass start key and end key. nil is also ok.
	GetRangeScanIterator(*tuple.Tuple, *tuple.Tuple, interface{}) IndexRangeScanIterator
}

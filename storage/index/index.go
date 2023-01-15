package index

import (
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
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
	table_name string
	// The mapping relation between key schema and tuple schema
	key_attrs []uint32
	// schema of the indexed key
	tuple_schema *schema.Schema
}

func NewIndexMetadata(index_name string, table_name string, tuple_schema *schema.Schema,
	key_attrs []uint32) *IndexMetadata {
	ret := new(IndexMetadata)
	ret.name = index_name
	ret.table_name = table_name
	ret.key_attrs = key_attrs
	//ret.tuple_schema = schema.CopySchema(tuple_schema, key_attrs)
	ret.tuple_schema = tuple_schema
	return ret
}

func (im *IndexMetadata) GetName() *string      { return &im.name }
func (im *IndexMetadata) GetTableName() *string { return &im.table_name }

// Returns a schema object pointer that represents the indexed key
func (im *IndexMetadata) GetTupleSchema() *schema.Schema { return im.tuple_schema }

// Return the number of columns inside index key (not in tuple key)
// Note that this must be defined inside the cpp source file
// because it uses the member of catalog::Schema which is not known here
func (im *IndexMetadata) GetIndexColumnCount() uint32 { return uint32(len(im.key_attrs)) }

// Returns the mapping relation between indexed columns  and base table
// columns
func (im *IndexMetadata) GetKeyAttrs() []uint32 { return im.key_attrs }

/*
   // Get a string representation for debugging
   std::string ToString() const {
	 std::stringstream os;

	 os << "IndexMetadata["
		<< "Name = " << name_ << ", "
		<< "Type = B+Tree, "
		<< "Table name = " << table_name_ << "] :: ";
	 os << key_schema_->ToString();

	 return os.str();
   }

  private:
*/

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

	/*
	      // Get a string representation for debugging
	      std::string ToString() const {
	   	 std::stringstream os;

	   	 os << "INDEX: (" << GetName() << ")";
	   	 os << metadata_->ToString();
	   	 return os.str();
	      }
	*/
}

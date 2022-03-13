package index

import (
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
)

// TODO: (SDB) need port IndexMetadata class
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
	key_schema *schema.Schema
}

func NewIndexMetadata(index_name string, table_name string, tuple_schema *schema.Schema,
	key_attrs []uint32) *IndexMetadata {
	ret := new(IndexMetadata)
	ret.name = index_name
	ret.table_name = table_name
	ret.key_attrs = key_attrs
	ret.key_schema = schema.CopySchema(tuple_schema, key_attrs)
}

func (im *IndexMetadata) GetName() *string      { return &im.name }
func (im *IndexMetadata) GetTableName() *string { return &im.table_name }

// Returns a schema object pointer that represents the indexed key
func (im *IndexMetadata) GetKeySchema() *schema.Schema { return &im.key_schema }

// Return the number of columns inside index key (not in tuple key)
// Note that this must be defined inside the cpp source file
// because it uses the member of catalog::Schema which is not known here
func (im *IndexMetadata) GetIndexColumnCount() uint32 { return uint32(im.key_attrs.size()) }

//  Returns the mapping relation between indexed columns  and base table
//  columns
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

// TODO: (SDB) need port Index class as interface
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
 * delete, predicate insert, point query, and full index scan. Predicate scan
 * only supports conjunction, and may or may not be optimized depending on
 * the type of expressions inside the predicate.
 */
/*
 class Index {
  public:
   explicit Index(IndexMetadata *metadata) : metadata_(metadata) {}

   virtual ~Index() { delete metadata_; }

   // Return the metadata object associated with the index
   IndexMetadata *GetMetadata() const { return metadata_; }

   int GetIndexColumnCount() const { return metadata_->GetIndexColumnCount(); }

   const std::string &GetName() const { return metadata_->GetName(); }

   Schema *GetKeySchema() const { return metadata_->GetKeySchema(); }

   const std::vector<uint32_t> &GetKeyAttrs() const { return metadata_->GetKeyAttrs(); }

   // Get a string representation for debugging
   std::string ToString() const {
	 std::stringstream os;

	 os << "INDEX: (" << GetName() << ")";
	 os << metadata_->ToString();
	 return os.str();
   }

   ///////////////////////////////////////////////////////////////////
   // Point Modification
   ///////////////////////////////////////////////////////////////////
   // designed for secondary indexes.
   virtual void InsertEntry(const Tuple &key, RID rid, Transaction *transaction) = 0;

   // delete the index entry linked to given tuple
   virtual void DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) = 0;

   virtual void ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) = 0;

  private:
   //===--------------------------------------------------------------------===//
   //  Data members
   //===--------------------------------------------------------------------===//
   IndexMetadata *metadata_;
 };
*/

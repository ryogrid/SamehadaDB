// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package catalog

import (
	"github.com/ryogrid/SamehadaDB/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
)

func TableCatalogSchema() *schema.Schema {
	oidColumn := column.NewColumn("oid", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	nameColumn := column.NewColumn("name", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	firstPageColumn := column.NewColumn("first_page", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	return schema.NewSchema([]*column.Column{oidColumn, nameColumn, firstPageColumn})
}

func ColumnsCatalogSchema() *schema.Schema {
	tableOIDColumn := column.NewColumn("table_oid", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	typeColumn := column.NewColumn("type", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	nameColumn := column.NewColumn("name", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	fixedLengthColumn := column.NewColumn("fixed_length", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	variableLengthColumn := column.NewColumn("variable_length", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	offsetColumn := column.NewColumn("offset", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	hasIndexColumn := column.NewColumn("has_index", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	indexKind := column.NewColumn("index_kind", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	indexHeaderPageId := column.NewColumn("index_header_page_id", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)

	return schema.NewSchema([]*column.Column{
		tableOIDColumn,
		typeColumn,
		nameColumn,
		fixedLengthColumn,
		variableLengthColumn,
		offsetColumn,
		hasIndexColumn,
		indexKind,
		indexHeaderPageId})
}

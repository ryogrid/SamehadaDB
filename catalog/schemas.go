// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package catalog

import (
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
)

func TableCatalogSchema() *schema.Schema {
	oidColumn := column.NewColumn("oid", types.Integer, false)
	nameColumn := column.NewColumn("name", types.Varchar, false)
	firstPageColumn := column.NewColumn("first_page", types.Integer, false)
	return schema.NewSchema([]*column.Column{oidColumn, nameColumn, firstPageColumn})
}

func ColumnsCatalogSchema() *schema.Schema {
	tableOIDColumn := column.NewColumn("table_oid", types.Integer, false)
	typeColumn := column.NewColumn("type", types.Integer, false)
	nameColumn := column.NewColumn("name", types.Varchar, false)
	fixedLengthColumn := column.NewColumn("fixed_length", types.Integer, false)
	variableLengthColumn := column.NewColumn("variable_length", types.Integer, false)
	offsetColumn := column.NewColumn("offset", types.Integer, false)
	hasIndexColumn := column.NewColumn("has_index", types.Integer, false)

	return schema.NewSchema([]*column.Column{
		tableOIDColumn,
		typeColumn,
		nameColumn,
		fixedLengthColumn,
		variableLengthColumn,
		offsetColumn,
		hasIndexColumn})
}

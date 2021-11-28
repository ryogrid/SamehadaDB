// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in license/go-bustub dir

package catalog

import (
	"github.com/ryogrid/SaitomDB/storage/table"
	"github.com/ryogrid/SaitomDB/types"
)

func TableCatalogSchema() *table.Schema {
	oidColumn := table.NewColumn("oid", types.Integer)
	nameColumn := table.NewColumn("name", types.Varchar)
	firstPageColumn := table.NewColumn("first_page", types.Integer)
	return table.NewSchema([]*table.Column{oidColumn, nameColumn, firstPageColumn})
}

func ColumnsCatalogSchema() *table.Schema {
	tableOIDColumn := table.NewColumn("table_oid", types.Integer)
	typeColumn := table.NewColumn("type", types.Integer)
	nameColumn := table.NewColumn("name", types.Varchar)
	fixedLengthColumn := table.NewColumn("fixed_length", types.Integer)
	variableLengthColumn := table.NewColumn("variable_length", types.Integer)
	offsetColumn := table.NewColumn("offset", types.Integer)

	return table.NewSchema([]*table.Column{
		tableOIDColumn,
		typeColumn,
		nameColumn,
		fixedLengthColumn,
		variableLengthColumn,
		offsetColumn,
	})
}

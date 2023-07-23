package plans

import (
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
)

func makeMergedOutputSchema(left_schema *schema.Schema, right_schema *schema.Schema) *schema.Schema {
	var ret *schema.Schema
	columns := make([]*column.Column, 0)
	for _, col := range left_schema.GetColumns() {
		columns = append(columns, col)
	}
	for _, col := range right_schema.GetColumns() {
		columns = append(columns, col)
	}
	ret = schema.NewSchema(columns)
	return ret
}

func constructOnExpressionFromKeysInfo(leftKeys []expression.Expression, rightKeys []expression.Expression) expression.Expression {
	if len(leftKeys) != 1 || len(rightKeys) != 1 {
		panic("constructOnExpressionFromKeysInfo supports only one key for left and right now.")
	}

	return expression.NewComparison(leftKeys[0], rightKeys[0], expression.Equal, types.Boolean)
}

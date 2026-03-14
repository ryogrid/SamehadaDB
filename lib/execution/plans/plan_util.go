package plans

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/column"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

func makeMergedOutputSchema(leftSchema *schema.Schema, rightSchema *schema.Schema) *schema.Schema {
	var ret *schema.Schema
	columns := make([]*column.Column, 0)
	for _, col := range leftSchema.GetColumns() {
		colCopy := *col
		colCopy.SetIsLeft(true)
		columns = append(columns, &colCopy)
	}
	for _, col := range rightSchema.GetColumns() {
		colCopy := *col
		colCopy.SetIsLeft(false)
		columns = append(columns, &colCopy)
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

func PrintPlanTree(plan Plan, indent int) {
	for ii := 0; ii < indent; ii++ {
		fmt.Print(" ")
	}
	fmt.Print(plan.GetDebugStr())
	fmt.Println("")

	for _, child := range plan.GetChildren() {
		PrintPlanTree(child, indent+2)
	}
}

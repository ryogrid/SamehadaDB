package execution

import (
	"github.com/brunocalza/go-bustub/storage/table"
	"github.com/brunocalza/go-bustub/types"
)

type ColumnValueExpression struct {
	tupleIndex uint32 // Tuple index 0 = left side of join, tuple index 1 = right side of join
	colIndex   uint32 // Column index refers to the index within the schema of the tuple, e.g. schema {A,B,C} has indexes {0,1,2}
}

func NewColumnValueExpression(tupleIndex uint32, colIndex uint32) Expression {
	return &ColumnValueExpression{tupleIndex, colIndex}
}

func (c *ColumnValueExpression) Evaluate(tuple *table.Tuple, schema *table.Schema) types.Value {
	return tuple.GetValue(schema, c.colIndex)
}

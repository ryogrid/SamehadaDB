package expression

import (
	"github.com/ryogrid/SaitomDB/storage/table"
	"github.com/ryogrid/SaitomDB/types"
)

type ColumnValue struct {
	tupleIndex uint32 // Tuple index 0 = left side of join, tuple index 1 = right side of join
	colIndex   uint32 // Column index refers to the index within the schema of the tuple, e.g. schema {A,B,C} has indexes {0,1,2}
}

func NewColumnValue(tupleIndex uint32, colIndex uint32) Expression {
	return &ColumnValue{tupleIndex, colIndex}
}

func (c *ColumnValue) Evaluate(tuple *table.Tuple, schema *table.Schema) types.Value {
	return tuple.GetValue(schema, c.colIndex)
}

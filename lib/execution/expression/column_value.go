// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package expression

import (
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"strings"
)

/**
 * ColumnValue maintains the tuple index and column index relative to a particular schema or join.
 */
type ColumnValue struct {
	*AbstractExpression
	tupleIndexForJoin uint32 // Tuple index 0 = left side of join, tuple index 1 = right side of join
	colIndex          uint32 // Column index refers to the index within the schema of the tuple, e.g. schema {A,B,C} has indexes {0,1,2}
}

func NewColumnValue(tupleIndex uint32, colIndex uint32, colType types.TypeID) Expression {
	return &ColumnValue{&AbstractExpression{[2]Expression{}, colType}, tupleIndex, colIndex}
}

func (c *ColumnValue) Evaluate(tuple *tuple.Tuple, schema *schema.Schema) types.Value {
	return tuple.GetValue(schema, c.colIndex)
}

func (c *ColumnValue) SetTupleIndex(tupleIndex uint32) {
	c.tupleIndexForJoin = tupleIndex
}

func (c *ColumnValue) SetColIndex(colIndex uint32) {
	c.colIndex = colIndex
}

func (c *ColumnValue) GetColIndex() uint32 {
	return c.colIndex
}

func (c *ColumnValue) EvaluateJoin(leftTuple *tuple.Tuple, leftSchema *schema.Schema, rightTuple *tuple.Tuple, rightSchema *schema.Schema) types.Value {
	if c.tupleIndexForJoin == 0 {
		return leftTuple.GetValue(leftSchema, c.colIndex)
	} else {
		return rightTuple.GetValue(rightSchema, c.colIndex)
	}
}

func (c *ColumnValue) EvaluateAggregate(groupBys []*types.Value, aggregates []*types.Value) types.Value {
	panic("Aggregation should only refer to group-by and aggregates.")
}

func (c *ColumnValue) GetChildAt(childIdx uint32) Expression {
	if int(childIdx) >= len(c.children) {
		return nil
	}
	return c.children[childIdx]
}

func (c *ColumnValue) GetReturnType() types.TypeID { return c.retType }

func (c *ColumnValue) SetReturnType(valueType types.TypeID) {
	c.retType = valueType
}

func MakeColumnValueExpression(sch *schema.Schema, tupleIdxOnJoin uint32,
	colName string) Expression {
	// note: alphabets on column name is stored in lowercase

	colIdx := sch.GetColIndex(strings.ToLower(colName))
	colType := sch.GetColumn(colIdx).GetType()
	colVal := NewColumnValue(tupleIdxOnJoin, colIdx, colType)
	return colVal
}

func (c *ColumnValue) GetType() ExpressionType {
	return ExpressionTypeColumnValue
}

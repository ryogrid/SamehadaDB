// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package expression

import (
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
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

func (c *ColumnValue) EvaluateJoin(left_tuple *tuple.Tuple, left_schema *schema.Schema, right_tuple *tuple.Tuple, right_schema *schema.Schema) types.Value {
	if c.tupleIndexForJoin == 0 {
		return left_tuple.GetValue(left_schema, c.colIndex)
	} else {
		return right_tuple.GetValue(right_schema, c.colIndex)
	}
}

func (c *ColumnValue) EvaluateAggregate(group_bys []*types.Value, aggregates []*types.Value) types.Value {
	panic("Aggregation should only refer to group-by and aggregates.")
}

func (c *ColumnValue) GetChildAt(child_idx uint32) Expression {
	if int(child_idx) >= len(c.children) {
		return nil
	}
	return c.children[child_idx]
}

func (c *ColumnValue) GetReturnType() types.TypeID { return c.ret_type }

func (c *ColumnValue) SetReturnType(valueType types.TypeID) {
	c.ret_type = valueType
}

func MakeColumnValueExpression(schema_ *schema.Schema, tuple_idx_on_join uint32,
	col_name string) Expression {
	// note: alphabets on column name is stored in lowercase

	col_idx := schema_.GetColIndex(strings.ToLower(col_name))
	col_type := schema_.GetColumn(col_idx).GetType()
	col_val := NewColumnValue(tuple_idx_on_join, col_idx, col_type)
	return col_val
}

func (c *ColumnValue) GetType() ExpressionType {
	return EXPRESSION_TYPE_COLUMN_VALUE
}

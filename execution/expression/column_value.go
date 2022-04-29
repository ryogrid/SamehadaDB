// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package expression

import (
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * ColumnValue maintains the tuple index and column index relative to a particular schema or join.
 */
type ColumnValue struct {
	*AbstractExpression
	tupleIndex uint32 // Tuple index 0 = left side of join, tuple index 1 = right side of join
	colIndex   uint32 // Column index refers to the index within the schema of the tuple, e.g. schema {A,B,C} has indexes {0,1,2}
}

func NewColumnValue(tupleIndex uint32, colIndex uint32, colType types.TypeID) Expression {
	return &ColumnValue{&AbstractExpression{[2]Expression{}, colType}, tupleIndex, colIndex}
}

func (c *ColumnValue) Evaluate(tuple *tuple.Tuple, schema *schema.Schema) types.Value {
	return tuple.GetValue(schema, c.colIndex)
}

func (c *ColumnValue) SetTupleIndex(tupleIndex uint32) {
	c.tupleIndex = tupleIndex
}

func (c *ColumnValue) SetColIndex(colIndex uint32) {
	c.colIndex = colIndex
}

func (c *ColumnValue) EvaluateJoin(left_tuple *tuple.Tuple, left_schema *schema.Schema, right_tuple *tuple.Tuple, right_schema *schema.Schema) types.Value {
	//return *new(types.Value)
	//panic("not implemented")
	if c.tupleIndex == 0 {
		return left_tuple.GetValue(left_schema, c.colIndex)
	} else {
		return right_tuple.GetValue(right_schema, c.colIndex)
	}
}

// TODO: (SDB) need to port ColumnValue::EvaluateAggregate method

// Value EvaluateAggregate(const std::vector<Value> &group_bys, const std::vector<Value> &aggregates) const override {
//     BUSTUB_ASSERT(false, "Aggregation should only refer to group-by and aggregates.");
//   }

func (c *ColumnValue) GetChildAt(child_idx uint32) Expression {
	//return nil
	//panic("not implemented")
	return c.children[child_idx]
}

func (c *ColumnValue) GetReturnType() types.TypeID { return c.ret_type }

func (c *ColumnValue) SetReturnType(valueType types.TypeID) {
	c.ret_type = valueType
}

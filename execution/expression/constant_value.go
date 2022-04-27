// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package expression

import (
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * ConstantValue represents constants.
 */
type ConstantValue struct {
	*AbstractExpression
	value types.Value
}

func NewConstantValue(value types.Value, colType types.TypeID) Expression {
	return &ConstantValue{&AbstractExpression{[2]Expression{}, colType}, value}
}

func (c *ConstantValue) Evaluate(tuple *tuple.Tuple, schema *schema.Schema) types.Value {
	return c.value
}

func (c *ConstantValue) EvaluateJoin(left_tuple *tuple.Tuple, left_schema *schema.Schema, right_tuple *tuple.Tuple, right_schema *schema.Schema) types.Value {
	//return *new(types.Value)
	//panic("not implemented")
	return c.value
}

// TODO: (SDB) need to port ConstantValue::EvaluateAggregate method

// Value EvaluateAggregate(const std::vector<Value> &group_bys, const std::vector<Value> &aggregates) const override {
//     return val_;
//   }

func (c *ConstantValue) GetChildAt(child_idx uint32) Expression {
	//return nil
	//panic("not implemented")
	return c.children[child_idx]
}

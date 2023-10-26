// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package expression

import (
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
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
	return c.value
}

func (c *ConstantValue) EvaluateAggregate(group_bys []*types.Value, aggregates []*types.Value) types.Value {
	return c.value
}

func (c *ConstantValue) GetChildAt(child_idx uint32) Expression {
	if int(child_idx) >= len(c.children) {
		return nil
	}
	return c.children[child_idx]
}

func (c *ConstantValue) GetType() ExpressionType {
	return EXPRESSION_TYPE_CONSTANT_VALUE
}

func (c *ConstantValue) GetValue() *types.Value {
	return &c.value
}

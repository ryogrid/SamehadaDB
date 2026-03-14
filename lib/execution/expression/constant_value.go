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

func (c *ConstantValue) EvaluateJoin(leftTuple *tuple.Tuple, leftSchema *schema.Schema, rightTuple *tuple.Tuple, rightSchema *schema.Schema) types.Value {
	return c.value
}

func (c *ConstantValue) EvaluateAggregate(groupBys []*types.Value, aggregates []*types.Value) types.Value {
	return c.value
}

func (c *ConstantValue) GetChildAt(childIdx uint32) Expression {
	if int(childIdx) >= len(c.children) {
		return nil
	}
	return c.children[childIdx]
}

func (c *ConstantValue) GetType() ExpressionType {
	return ExpressionTypeConstantValue
}

func (c *ConstantValue) GetValue() *types.Value {
	return &c.value
}

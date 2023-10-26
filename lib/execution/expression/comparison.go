// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package expression

import (
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

type ComparisonType int

/** ComparisonType represents the type of comparison that we want to perform. */
const (
	Equal ComparisonType = iota
	NotEqual
	GreaterThan        // A > B
	GreaterThanOrEqual // A >= B
	LessThan           // A < B
	LessThanOrEqual    // A <= B
)

/**
 * ComparisonExpression represents two expressions being compared.
 */
type Comparison struct {
	*AbstractExpression
	comparisonType ComparisonType
}

func NewComparison(left Expression, right Expression, comparisonType ComparisonType, colType types.TypeID) Expression {
	ret := &Comparison{&AbstractExpression{[2]Expression{left, right}, colType}, comparisonType}
	return ret
}

func (c *Comparison) Evaluate(tuple_ *tuple.Tuple, schema_ *schema.Schema) types.Value {
	if c == nil {
		return types.NewBoolean(false)
	}
	lhs := c.children[0].Evaluate(tuple_, schema_)
	rhs := c.children[1].Evaluate(tuple_, schema_)
	return types.NewBoolean(c.performComparison(lhs, rhs))
}

func (c *Comparison) performComparison(lhs types.Value, rhs types.Value) bool {
	switch c.comparisonType {
	case Equal:
		return lhs.CompareEquals(rhs)
	case NotEqual:
		return lhs.CompareNotEquals(rhs)
	case GreaterThan:
		return lhs.CompareGreaterThan(rhs)
	case GreaterThanOrEqual:
		return lhs.CompareGreaterThanOrEqual(rhs)
	case LessThan:
		return lhs.CompareLessThan(rhs)
	case LessThanOrEqual:
		return lhs.CompareLessThanOrEqual(rhs)
	default:
		panic("illegal comparisonType is passed!")
	}

}

func (c *Comparison) GetLeftSideColIdx() uint32 {
	return c.children[0].(*ColumnValue).colIndex
}

func (c *Comparison) GetRightSideValue(tuple_ *tuple.Tuple, schema_ *schema.Schema) types.Value {
	return c.children[1].Evaluate(tuple_, schema_)
}

func (c *Comparison) GetComparisonType() ComparisonType {
	return c.comparisonType
}

func (c *Comparison) EvaluateJoin(left_tuple *tuple.Tuple, left_schema *schema.Schema, right_tuple *tuple.Tuple, right_schema *schema.Schema) types.Value {
	lhs := c.GetChildAt(0).EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema)
	rhs := c.GetChildAt(1).EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema)
	return types.NewBoolean(c.performComparison(lhs, rhs))
}

func (c *Comparison) EvaluateAggregate(group_bys []*types.Value, aggregates []*types.Value) types.Value {
	lhs := c.GetChildAt(0).EvaluateAggregate(group_bys, aggregates)
	rhs := c.GetChildAt(1).EvaluateAggregate(group_bys, aggregates)
	return types.NewBoolean(c.performComparison(lhs, rhs))
}

func (c *Comparison) GetChildAt(child_idx uint32) Expression {
	if int(child_idx) >= len(c.children) {
		return nil
	}
	return c.children[child_idx]
}

func (c *Comparison) SetChildAt(child_idx uint32, child Expression) {
	c.children[child_idx] = child
}

func (c *Comparison) GetType() ExpressionType {
	return EXPRESSION_TYPE_COMPARISON
}

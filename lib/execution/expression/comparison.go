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

func (c *Comparison) Evaluate(tpl *tuple.Tuple, sch *schema.Schema) types.Value {
	if c == nil {
		return types.NewBoolean(false)
	}
	lhs := c.children[0].Evaluate(tpl, sch)
	rhs := c.children[1].Evaluate(tpl, sch)
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

func (c *Comparison) GetRightSideValue(tpl *tuple.Tuple, sch *schema.Schema) types.Value {
	return c.children[1].Evaluate(tpl, sch)
}

func (c *Comparison) GetComparisonType() ComparisonType {
	return c.comparisonType
}

func (c *Comparison) EvaluateJoin(leftTuple *tuple.Tuple, leftSchema *schema.Schema, rightTuple *tuple.Tuple, rightSchema *schema.Schema) types.Value {
	lhs := c.GetChildAt(0).EvaluateJoin(leftTuple, leftSchema, rightTuple, rightSchema)
	rhs := c.GetChildAt(1).EvaluateJoin(leftTuple, leftSchema, rightTuple, rightSchema)
	return types.NewBoolean(c.performComparison(lhs, rhs))
}

func (c *Comparison) EvaluateAggregate(groupBys []*types.Value, aggregates []*types.Value) types.Value {
	lhs := c.GetChildAt(0).EvaluateAggregate(groupBys, aggregates)
	rhs := c.GetChildAt(1).EvaluateAggregate(groupBys, aggregates)
	return types.NewBoolean(c.performComparison(lhs, rhs))
}

func (c *Comparison) GetChildAt(childIdx uint32) Expression {
	if int(childIdx) >= len(c.children) {
		return nil
	}
	return c.children[childIdx]
}

func (c *Comparison) SetChildAt(childIdx uint32, child Expression) {
	c.children[childIdx] = child
}

func (c *Comparison) GetType() ExpressionType {
	return ExpressionTypeComparison
}

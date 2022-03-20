// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package expression

import (
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

type ComparisonType int

/** ComparisonType represents the type of comparison that we want to perform. */
const (
	Equal ComparisonType = iota
	NotEqual
)

/**
 * ComparisonExpression represents two expressions being compared.
 */
type Comparison struct {
	comparisonType ComparisonType
	//children       []Expression
	children_left  ColumnValue
	children_right Expression
}

//func NewComparison(left Expression, right Expression, comparisonType ComparisonType) Expression {
func NewComparison(left ColumnValue, right Expression, comparisonType ComparisonType) Expression {
	//children := make([]Expression, 2)
	// children[0] = left
	// children[1] = right

	//return &Comparison{comparisonType, children}
	return &Comparison{comparisonType, left, right}
}

//func NewComparisonAsComparison(left Expression, right Expression, comparisonType ComparisonType) *Comparison {
func NewComparisonAsComparison(left ColumnValue, right Expression, comparisonType ComparisonType) *Comparison {
	// children := make([]Expression, 2)
	// children[0] = left
	// children[1] = right
	// return &Comparison{comparisonType, children}
	return &Comparison{comparisonType, left, right}
}

func (c *Comparison) Evaluate(tuple *tuple.Tuple, schema *schema.Schema) types.Value {
	// lhs := c.children[0].Evaluate(tuple, schema)
	// rhs := c.children[1].Evaluate(tuple, schema)
	lhs := c.children_left.Evaluate(tuple, schema)
	rhs := c.children_right.Evaluate(tuple, schema)
	return types.NewBoolean(c.performComparison(lhs, rhs))
}

func (c *Comparison) performComparison(lhs types.Value, rhs types.Value) bool {
	switch c.comparisonType {
	case Equal:
		return lhs.CompareEquals(rhs)
	case NotEqual:
		return lhs.CompareNotEquals(rhs)
	}
	return false
}

// func (c *Comparison) GetLeftSideValue(tuple *tuple.Tuple, schema *schema.Schema) types.Value {
// 	return c.children[0].Evaluate(tuple, schema)
// }

func (c *Comparison) GetLeftSideColIdx() uint32 {
	//return (*ColumnValue)(unsafe.Pointer(&c.children[0])).colIndex
	return c.children_left.colIndex
}

func (c *Comparison) GetRightSideValue(tuple *tuple.Tuple, schema *schema.Schema) types.Value {
	//return c.children[1].Evaluate(tuple, schema)
	return c.children_right.Evaluate(tuple, schema)
}

func (c *Comparison) GetComparisonType() ComparisonType {
	return c.comparisonType
}

package execution

import (
	"github.com/brunocalza/go-bustub/storage/table"
	"github.com/brunocalza/go-bustub/types"
)

type ComparisonType int

const (
	Equal ComparisonType = iota
	NotEqual
)

// type CompBool int

// const (
// 	CmpFalse CompBool = iota
// 	CmpTrue
// 	CmpNull
// )

type ComparisonExpression struct {
	comparisonType ComparisonType
	children       []Expression
}

func NewComparisonExpression(left Expression, right Expression, comparisonType ComparisonType) Expression {
	children := make([]Expression, 2)
	children[0] = left
	children[1] = right
	return &ComparisonExpression{comparisonType, children}
}

func (c *ComparisonExpression) Evaluate(tuple *table.Tuple, schema *table.Schema) types.Value {
	lhs := c.children[0].Evaluate(tuple, schema)
	rhs := c.children[1].Evaluate(tuple, schema)
	return types.NewBooleanType(c.performComparison(lhs, rhs))
}

func (c *ComparisonExpression) performComparison(lhs types.Value, rhs types.Value) bool {
	switch c.comparisonType {
	case Equal:
		return lhs.(types.IntegerType).CompareEquals(rhs.(types.IntegerType))
	case NotEqual:
		return lhs.(types.IntegerType).CompareNotEquals(rhs.(types.IntegerType))
	}
	return false
}

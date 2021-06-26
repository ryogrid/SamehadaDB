package execution

import (
	"github.com/brunocalza/go-bustub/storage/table"
	"github.com/brunocalza/go-bustub/types"
)

type ConstantValueExpression struct {
	value types.Value
}

func NewConstantValueExpression(value types.Value) Expression {
	return &ConstantValueExpression{value}
}

func (c *ConstantValueExpression) Evaluate(tuple *table.Tuple, schema *table.Schema) types.Value {
	return c.value
}

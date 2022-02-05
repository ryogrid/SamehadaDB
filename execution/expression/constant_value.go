// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package expression

import (
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

type ConstantValue struct {
	value types.Value
}

func NewConstantValue(value types.Value) Expression {
	return &ConstantValue{value}
}

func (c *ConstantValue) Evaluate(tuple *tuple.Tuple, schema *schema.Schema) types.Value {
	return c.value
}

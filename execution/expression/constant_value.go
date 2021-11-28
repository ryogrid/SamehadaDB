// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package expression

import (
	"github.com/ryogrid/SaitomDB/storage/table"
	"github.com/ryogrid/SaitomDB/types"
)

type ConstantValue struct {
	value types.Value
}

func NewConstantValue(value types.Value) Expression {
	return &ConstantValue{value}
}

func (c *ConstantValue) Evaluate(tuple *table.Tuple, schema *table.Schema) types.Value {
	return c.value
}

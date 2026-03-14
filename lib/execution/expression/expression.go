// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package expression

import (
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

type ExpressionType int

const (
	ExpressionTypeInvalid ExpressionType = iota
	ExpressionTypeAggregateValue
	ExpressionTypeComparison
	ExpressionTypeColumnValue
	ExpressionTypeConstantValue
	ExpressionTypeLogicalOp
)

/**
 * Expression interface is the base of all the expressions in the system.
 * Expressions are modeled as trees, i.e. every expression may have a variable number of children.
 */
type Expression interface {
	Evaluate(*tuple.Tuple, *schema.Schema) types.Value
	GetChildAt(uint32) Expression
	EvaluateJoin(*tuple.Tuple, *schema.Schema, *tuple.Tuple, *schema.Schema) types.Value
	EvaluateAggregate([]*types.Value, []*types.Value) types.Value
	GetType() ExpressionType
}

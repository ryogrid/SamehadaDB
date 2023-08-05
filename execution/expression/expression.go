// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package expression

import (
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

type ExpressionType int

const (
	EXPRESSION_TYPE_INVALID ExpressionType = iota
	EXPRESSION_TYPE_AGGREGATE_VALUE
	EXPRESSION_TYPE_COMPARISON
	EXPRESSION_TYPE_COLUMN_VALUE
	EXPRESSION_TYPE_CONSTANT_VALUE
	EXPRESSION_TYPE_LOGICAL_OP
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

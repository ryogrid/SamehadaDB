package expression

import (
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

/**
 * AggregateValueExpression represents aggregations such as MAX(a), MIN(b), COUNT(c)
 */
type AggregateValueExpression struct {
	*AbstractExpression
	isGroupByTerm bool
	termIdx         uint32
}

/**
 * Creates a new AggregateValueExpression.
 * @param isGroupByTerm true if this is a group by
 * @param termIdx the index of the term
 * @param retType the return type of the aggregate value expression
 */

func NewAggregateValueExpression(isGroupByTerm bool, termIdx uint32, retType types.TypeID) Expression {
	return &AggregateValueExpression{&AbstractExpression{[2]Expression{}, retType}, isGroupByTerm, termIdx}
}

func (a *AggregateValueExpression) Evaluate(tuple *tuple.Tuple, schema *schema.Schema) types.Value {
	panic("Aggregation should only refer to group-by and aggregates.")
}

func (a *AggregateValueExpression) EvaluateJoin(leftTuple *tuple.Tuple, leftSchema *schema.Schema, rightTuple *tuple.Tuple, rightSchema *schema.Schema) types.Value {
	panic("Aggregation should only refer to group-by and aggregates.")
}

func (a *AggregateValueExpression) EvaluateAggregate(groupBys []*types.Value, aggregates []*types.Value) types.Value {
	if a.isGroupByTerm {
		return *groupBys[a.termIdx]
	} else {
		return *aggregates[a.termIdx]
	}
}

func (a *AggregateValueExpression) GetChildAt(childIdx uint32) Expression {
	if int(childIdx) >= len(a.children) {
		return nil
	}
	return a.children[childIdx]
}

func (a *AggregateValueExpression) GetReturnType() types.TypeID {
	return a.retType
}

func (a *AggregateValueExpression) GetType() ExpressionType {
	return ExpressionTypeAggregateValue
}

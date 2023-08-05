package expression

import (
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * AggregateValueExpression represents aggregations such as MAX(a), MIN(b), COUNT(c)
 */
type AggregateValueExpression struct {
	*AbstractExpression
	is_group_by_term_ bool
	term_idx_         uint32
}

/**
 * Creates a new AggregateValueExpression.
 * @param is_group_by_term true if this is a group by
 * @param term_idx the index of the term
 * @param ret_type the return type of the aggregate value expression
 */

func NewAggregateValueExpression(is_group_by_term bool, term_idx uint32, ret_type types.TypeID) Expression {
	return &AggregateValueExpression{&AbstractExpression{[2]Expression{}, ret_type}, is_group_by_term, term_idx}
}

func (a *AggregateValueExpression) Evaluate(tuple *tuple.Tuple, schema *schema.Schema) types.Value {
	panic("Aggregation should only refer to group-by and aggregates.")
}

func (a *AggregateValueExpression) EvaluateJoin(left_tuple *tuple.Tuple, left_schema *schema.Schema, right_tuple *tuple.Tuple, right_schema *schema.Schema) types.Value {
	panic("Aggregation should only refer to group-by and aggregates.")
}

func (a *AggregateValueExpression) EvaluateAggregate(group_bys []*types.Value, aggregates []*types.Value) types.Value {
	if a.is_group_by_term_ {
		return *group_bys[a.term_idx_]
	} else {
		return *aggregates[a.term_idx_]
	}
}

func (a *AggregateValueExpression) GetChildAt(child_idx uint32) Expression {
	return a.children[child_idx]
}

func (a *AggregateValueExpression) GetReturnType() types.TypeID {
	return a.ret_type
}

func (a *AggregateValueExpression) GetType() ExpressionType {
	return EXPRESSION_TYPE_AGGREGATE_VALUE
}

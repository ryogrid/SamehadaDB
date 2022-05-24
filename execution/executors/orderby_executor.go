package executors

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
* OrderbyExecutor executes an aggregation operation (e.g. COUNT, SUM, MIN, MAX) on the tuples of a child executor.
 */
type OrderbyExecutor struct {
	context *ExecutorContext
	/** The aggregation plan node. */
	plan_ *plans.OrderbyPlanNode
	/** The child executor whose tuples we are aggregating. */
	child_ []Executor
	exprs_ []expression.Expression
}

/**
 * Creates a new aggregation executor.
 * @param exec_ctx the context that the aggregation should be performed in
 * @param plan the aggregation plan node
 * @param child the child executor
 */
func NewOrderbyExecutor(exec_ctx *ExecutorContext, plan *plans.AggregationPlanNode,
	child Executor) *OrderbyExecutor {
	aht := NewSimpleAggregationHashTable(plan.GetAggregates(), plan.GetAggregateTypes())
	return &OrderbyExecutor{exec_ctx, plan, []Executor{child}, aht, nil, []expression.Expression{}}
}

//  /** Do not use or remove this function, otherwise you will get zero points. */
//   AbstractExecutor *GetChildExecutor()  { return child_.get() }

func (e *OrderbyExecutor) GetOutputSchema() *schema.Schema { return e.plan_.OutputSchema() }

func (e *OrderbyExecutor) Init() {
	//Tuple tuple
	e.child_[0].Init()
	child_exec := e.child_[0]
	output_column_cnt := int(e.GetOutputSchema().GetColumnCount())
	for i := 0; i < output_column_cnt; i++ {
		agg_expr := e.GetOutputSchema().GetColumn(uint32(i)).GetExpr().(expression.AggregateValueExpression)
		e.exprs_ = append(e.exprs_, &agg_expr)
	}
	insert_call_cnt := 0
	for {
		tuple_, done, err := child_exec.Next()
		if err != nil || done {
			if err != nil {
				fmt.Println(err)
			}
			break
		}

		if tuple_ != nil {
			e.aht_.InsertCombine(e.MakeKey(tuple_), e.MakeVal(tuple_))
			insert_call_cnt++
		}
	}
	fmt.Printf("insert_call_cnt %d\n", insert_call_cnt)
	e.aht_iterator_ = e.aht_.Begin()
}

func (e *OrderbyExecutor) Next() (*tuple.Tuple, Done, error) {
	for !e.aht_iterator_.IsNextEnd() && e.plan_.GetHaving() != nil && !e.plan_.GetHaving().EvaluateAggregate(e.aht_iterator_.Key().Group_bys_, e.aht_iterator_.Val().Aggregates_).ToBoolean() {
		//++aht_iterator_;
		e.aht_iterator_.Next()
	}
	if e.aht_iterator_.IsEnd() {
		return nil, true, nil
	}
	var values []types.Value = make([]types.Value, 0)
	for i := 0; i < len(e.exprs_); i++ {
		values = append(values, e.exprs_[i].EvaluateAggregate(e.aht_iterator_.Key().Group_bys_, e.aht_iterator_.Val().Aggregates_))
	}
	tuple_ := tuple.NewTupleFromSchema(values, e.GetOutputSchema())
	//++aht_iterator_;
	e.aht_iterator_.Next()
	return tuple_, false, nil
}

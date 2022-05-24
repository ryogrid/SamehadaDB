package executors

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
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
	//exprs_ []expression.Expression
	sort_tuples_ []*tuple.Tuple
	cur_idx_     int // target tuple index on Next method
}

/**
 * Creates a new aggregation executor.
 * @param exec_ctx the context that the aggregation should be performed in
 * @param plan the aggregation plan node
 * @param child the child executor
 */
func NewOrderbyExecutor(exec_ctx *ExecutorContext, plan *plans.OrderbyPlanNode,
	child Executor) *OrderbyExecutor {
	return &OrderbyExecutor{exec_ctx, plan, []Executor{child}, make([]*tuple.Tuple, 0), 0}
}

func (e *OrderbyExecutor) GetOutputSchema() *schema.Schema { return e.plan_.OutputSchema() }

func (e *OrderbyExecutor) Init() {
	e.child_[0].Init()
	child_exec := e.child_[0]
	inserted_tuple_cnt := 0
	for {
		tuple_, done, err := child_exec.Next()
		if err != nil || done {
			if err != nil {
				fmt.Println(err)
			}
			break
		}

		if tuple_ != nil {
			e.sort_tuples_ = append(e.sort_tuples_, tuple_)
			inserted_tuple_cnt++
		}
	}
	fmt.Printf("inserted_tuple_cnt %d\n", inserted_tuple_cnt)
}

func (e *OrderbyExecutor) Next() (*tuple.Tuple, Done, error) {
	//for !e.aht_iterator_.IsNextEnd() && e.plan_.GetHaving() != nil && !e.plan_.GetHaving().EvaluateAggregate(e.aht_iterator_.Key().Group_bys_, e.aht_iterator_.Val().Aggregates_).ToBoolean() {
	//	//++aht_iterator_;
	//	e.aht_iterator_.Next()
	//}
	//if e.aht_iterator_.IsEnd() {
	//	return nil, true, nil
	//}
	//var values []types.Value = make([]types.Value, 0)
	//for i := 0; i < len(e.exprs_); i++ {
	//	values = append(values, e.exprs_[i].EvaluateAggregate(e.aht_iterator_.Key().Group_bys_, e.aht_iterator_.Val().Aggregates_))
	//}
	//tuple_ := tuple.NewTupleFromSchema(values, e.GetOutputSchema())
	////++aht_iterator_;
	//e.aht_iterator_.Next()
	return tuple_, false, nil
}

package executors

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"sort"
)

/**
* OrderbyExecutor executes an aggregation operation (e.g. COUNT, SUM, MIN, MAX) on the tuples of a child executor.
 */
type OrderbyExecutor struct {
	context *ExecutorContext
	/** The aggregation plan node. */
	plan_ *plans.OrderbyPlanNode
	/** The child executor whose tuples we are aggregating. */
	child_       []Executor
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

func (e *OrderbyExecutor) GetChildOutputSchema() *schema.Schema { return e.child_[0].GetOutputSchema() }

func (e *OrderbyExecutor) Init() {
	e.child_[0].Init()
	child_exec := e.child_[0]
	child_schema := e.GetChildOutputSchema()
	sort_values := make([][]*types.Value, 0)
	inserted_tuple_cnt := int32(0)
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
			tmp_row := make([]*types.Value, 0)
			for _, col_idx := range e.plan_.GetColIdxs() {
				tmp_val := tuple_.GetValue(child_schema, uint32(col_idx))
				tmp_row = append(tmp_row, &tmp_val)
			}
			// add idx of before sort at end of col vals
			tmp_val := types.NewInteger(inserted_tuple_cnt)
			tmp_row = append(tmp_row, &tmp_val)
			sort_values = append(sort_values, tmp_row)
		}
		inserted_tuple_cnt++
	}
	// decide tuple order by sort of values on tuples
	sort.Slice(sort_values, func(i, j int) bool {

		cols_num := len(e.plan_.GetColIdxs())
		for idx := 0; idx < cols_num; idx++ {
			order_type := e.plan_.GetOrderbyTypes()[idx]
			if order_type == plans.ASC {
				if sort_values[i][idx].CompareEquals(*sort_values[j][idx]) {
					continue
				} else if sort_values[i][idx].CompareLessThan(*sort_values[j][idx]) { // i < j
					return true
				} else { // i > j
					return false
				}
			} else { //DESC
				if sort_values[i][idx].CompareEquals(*sort_values[j][idx]) {
					continue
				} else if sort_values[i][idx].CompareLessThan(*sort_values[j][idx]) { // i < j
					return false
				} else { // i > j
					return true
				}
			}
		}
		return false
	})
	fmt.Printf("inserted_tuple_cnt %d\n", inserted_tuple_cnt)
	// arrange tuple array (apply sort result)
	tuple_cnt := len(e.sort_tuples_)
	var tmp_tuples []*tuple.Tuple = make([]*tuple.Tuple, tuple_cnt)
	idx_of_orig_idx := len(e.plan_.GetColIdxs())
	for idx := 0; idx < tuple_cnt; idx++ {
		tmp_tuples[idx] = e.sort_tuples_[sort_values[idx][idx_of_orig_idx].ToInteger()]
	}
	// set sorted result to field
	e.sort_tuples_ = tmp_tuples
}

func (e *OrderbyExecutor) Next() (*tuple.Tuple, Done, error) {
	if e.cur_idx_ < len(e.sort_tuples_) {
		ret := e.sort_tuples_[e.cur_idx_]
		e.cur_idx_++
		return ret, false, nil
	} else {
		return nil, true, nil
	}
}

func (e *OrderbyExecutor) GetTableMetaData() *catalog.TableMetadata {
	return e.child_[0].GetTableMetaData()
}

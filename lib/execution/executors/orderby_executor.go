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
	plan *plans.OrderbyPlanNode
	/** The child executor whose tuples we are aggregating. */
	child       []Executor
	sortTuples []*tuple.Tuple
	curIdx     int // target tuple index on Next method
}

/**
 * Creates a new aggregation executor.
 * @param execCtx the context that the aggregation should be performed in
 * @param plan the aggregation plan node
 * @param child the child executor
 */
func NewOrderbyExecutor(execCtx *ExecutorContext, plan *plans.OrderbyPlanNode,
	child Executor) *OrderbyExecutor {
	return &OrderbyExecutor{execCtx, plan, []Executor{child}, make([]*tuple.Tuple, 0), 0}
}

func (e *OrderbyExecutor) GetOutputSchema() *schema.Schema { return e.plan.OutputSchema() }

func (e *OrderbyExecutor) GetChildOutputSchema() *schema.Schema { return e.child[0].GetOutputSchema() }

func (e *OrderbyExecutor) Init() {
	e.child[0].Init()
	childExec := e.child[0]
	childSchema := e.GetChildOutputSchema()
	sortValues := make([][]*types.Value, 0)
	insertedTupleCnt := int32(0)
	for {
		tpl, done, err := childExec.Next()
		if err != nil || done {
			if err != nil {
				fmt.Println(err)
			}
			break
		}

		if tpl != nil {
			e.sortTuples = append(e.sortTuples, tpl)
			tmpRow := make([]*types.Value, 0)
			for _, colIdx := range e.plan.GetColIdxs() {
				tmpVal := tpl.GetValue(childSchema, uint32(colIdx))
				tmpRow = append(tmpRow, &tmpVal)
			}
			// add idx of before sort at end of col vals
			tmpVal := types.NewInteger(insertedTupleCnt)
			tmpRow = append(tmpRow, &tmpVal)
			sortValues = append(sortValues, tmpRow)
		}
		insertedTupleCnt++
	}
	// decide tuple order by sort of values on tuples
	sort.Slice(sortValues, func(i, j int) bool {

		colsNum := len(e.plan.GetColIdxs())
		for idx := 0; idx < colsNum; idx++ {
			orderType := e.plan.GetOrderbyTypes()[idx]
			if orderType == plans.ASC {
				if sortValues[i][idx].CompareEquals(*sortValues[j][idx]) {
					continue
				} else if sortValues[i][idx].CompareLessThan(*sortValues[j][idx]) { // i < j
					return true
				} else { // i > j
					return false
				}
			} else { //DESC
				if sortValues[i][idx].CompareEquals(*sortValues[j][idx]) {
					continue
				} else if sortValues[i][idx].CompareLessThan(*sortValues[j][idx]) { // i < j
					return false
				} else { // i > j
					return true
				}
			}
		}
		return false
	})
	fmt.Printf("insertedTupleCnt %d\n", insertedTupleCnt)
	// arrange tuple array (apply sort result)
	tupleCnt := len(e.sortTuples)
	var tmpTuples = make([]*tuple.Tuple, tupleCnt)
	idxOfOrigIdx := len(e.plan.GetColIdxs())
	for idx := 0; idx < tupleCnt; idx++ {
		tmpTuples[idx] = e.sortTuples[sortValues[idx][idxOfOrigIdx].ToInteger()]
	}
	// set sorted result to field
	e.sortTuples = tmpTuples
}

func (e *OrderbyExecutor) Next() (*tuple.Tuple, Done, error) {
	if e.curIdx < len(e.sortTuples) {
		ret := e.sortTuples[e.curIdx]
		e.curIdx++
		return ret, false, nil
	} else {
		return nil, true, nil
	}
}

func (e *OrderbyExecutor) GetTableMetaData() *catalog.TableMetadata {
	return e.child[0].GetTableMetaData()
}

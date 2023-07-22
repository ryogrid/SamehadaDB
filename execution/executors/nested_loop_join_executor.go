package executors

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

type NestedLoopJoinExecutor struct {
	context   *ExecutorContext
	plan      *plans.NestedLoopJoinPlanNode
	left      Executor
	right     Executor
	retTuples []*tuple.Tuple
	curIdx    int32
}

func NewNestedLoopJoinExecutor(exec_ctx *ExecutorContext, plan *plans.NestedLoopJoinPlanNode, left Executor,
	right Executor) *NestedLoopJoinExecutor {
	ret := new(NestedLoopJoinExecutor)
	ret.plan = plan
	ret.left = left
	ret.right = right
	ret.context = exec_ctx
	ret.retTuples = make([]*tuple.Tuple, 0)
	return ret
}

func (e *NestedLoopJoinExecutor) GetOutputSchema() *schema.Schema { return e.plan.OutputSchema() }

func (e *NestedLoopJoinExecutor) Init() {
	e.left.Init()
	e.right.Init()

	rightTuples := make([]*tuple.Tuple, 0)
	for rightTuple, doneRight, errRight := e.right.Next(); !doneRight; rightTuple, doneRight, errRight = e.right.Next() {
		if errRight != nil {
			e.context.txn.SetState(access.ABORTED)
			return
		}
		rightTuples = append(rightTuples, rightTuple)
	}

	for leftTuple, doneLeft, errLeft := e.left.Next(); !doneLeft; leftTuple, doneLeft, errLeft = e.left.Next() {
		if errLeft != nil {
			e.context.txn.SetState(access.ABORTED)
			return
		}
		for _, rightTuple := range rightTuples {
			e.retTuples = append(e.retTuples, e.MakeOutputTuple(leftTuple, rightTuple))
		}
	}
}

// TODO: (SDB) need to refactor NestedLoopJoinExecutor::Next method to use GetExpr method of Column class

func (e *NestedLoopJoinExecutor) Next() (*tuple.Tuple, Done, error) {
	if e.curIdx >= int32(len(e.retTuples)) {
		return nil, true, nil
	}
	ret := e.retTuples[e.curIdx]
	e.curIdx++
	return ret, false, nil
}

func (e *NestedLoopJoinExecutor) MakeOutputTuple(left_tuple *tuple.Tuple, right_tuple *tuple.Tuple) *tuple.Tuple {
	outputColumnCnt := int(e.GetOutputSchema().GetColumnCount())
	leftColumnCnt := int(e.left.GetOutputSchema().GetColumnCount())
	values := make([]types.Value, outputColumnCnt)
	for ii := 0; ii < outputColumnCnt; ii++ {
		if ii < leftColumnCnt {
			values[ii] = left_tuple.GetValue(e.left.GetOutputSchema(), uint32(ii))
		} else {
			values[ii] = right_tuple.GetValue(e.right.GetOutputSchema(), uint32(ii-leftColumnCnt))
		}
	}
	return tuple.NewTupleFromSchema(values, e.GetOutputSchema())
}

// can not be used
func (e *NestedLoopJoinExecutor) GetTableMetaData() *catalog.TableMetadata { return nil }

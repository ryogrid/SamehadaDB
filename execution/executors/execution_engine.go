// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package executors

import (
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
)

// ExecutionEngine is the query execution engine.
//
// It is an implementation of the GetRangeScanIterator Model (also called Pipeline model or Volcano)
// It receives a Plan, create a Executor for that plan and execute it
// All executors follow the same pattern implementing the Executor interface
// Executors are the operators in relation algebra
type ExecutionEngine struct {
}

func (e *ExecutionEngine) Execute(plan plans.Plan, context *ExecutorContext) []*tuple.Tuple {
	executor := e.CreateExecutor(plan, context)
	executor.Init()

	tuples := []*tuple.Tuple{}
	for {
		tuple, done, err := executor.Next()
		if err != nil {
			context.txn.SetState(access.ABORTED)
			return nil
		}
		if done {
			break
		}

		if tuple != nil {
			tuples = append(tuples, tuple)
		}
	}

	return tuples
}

func (e *ExecutionEngine) CreateExecutor(plan plans.Plan, context *ExecutorContext) Executor {
	switch p := plan.(type) {
	case *plans.InsertPlanNode:
		return NewInsertExecutor(context, p)
	case *plans.SeqScanPlanNode:
		return NewSeqScanExecutor(context, p)
	case *plans.PointScanWithIndexPlanNode:
		return NewPointScanWithIndexExecutor(context, p)
	case *plans.RangeScanWithIndexPlanNode:
		return NewRangeScanWithIndexExecutor(context, p)
	case *plans.LimitPlanNode:
		return NewLimitExecutor(context, p, e.CreateExecutor(plan.GetChildAt(0), context))
	case *plans.DeletePlanNode:
		return NewDeleteExecutor(context, p, e.CreateExecutor(plan.GetChildAt(0), context))
	case *plans.UpdatePlanNode:
		return NewUpdateExecutor(context, p, e.CreateExecutor(plan.GetChildAt(0), context))
	case *plans.HashJoinPlanNode:
		return NewHashJoinExecutor(context, p, e.CreateExecutor(plan.GetChildAt(0), context), e.CreateExecutor(plan.GetChildAt(1), context))
	case *plans.IndexJoinPlanNode:
		return NewIndexJoinExecutor(context, p, e.CreateExecutor(plan.GetChildAt(0), context))
	case *plans.NestedLoopJoinPlanNode:
		return NewNestedLoopJoinExecutor(context, p, e.CreateExecutor(plan.GetChildAt(0), context), e.CreateExecutor(plan.GetChildAt(1), context))
	case *plans.AggregationPlanNode:
		return NewAggregationExecutor(context, p, e.CreateExecutor(plan.GetChildAt(0), context))
	case *plans.OrderbyPlanNode:
		return NewOrderbyExecutor(context, p, e.CreateExecutor(plan.GetChildAt(0), context))
	case *plans.SelectionPlanNode:
		return NewSelectionExecutor(context, p, e.CreateExecutor(plan.GetChildAt(0), context))
	case *plans.ProjectionPlanNode:
		return NewProjectionExecutor(context, p, e.CreateExecutor(plan.GetChildAt(0), context))
	}
	return nil
}

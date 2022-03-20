// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package executors

import (
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
)

// ExecutionEngine is the query execution engine.
//
// It is an implementation of the Iterator Model (also called Pipeline model or Volcano)
// It receives a Plan, create a Executor for that plan and execute it
// All executors follow the same pattern implementing the Executor interface
// Executors are the operators in relation algebra
type ExecutionEngine struct {
}

func (e *ExecutionEngine) Execute(plan plans.Plan, context *ExecutorContext) []*tuple.Tuple {
	executor := e.createExecutor(plan, context)
	executor.Init()

	tuples := []*tuple.Tuple{}
	for {
		tuple, done, err := executor.Next()
		if err != nil || done {
			break
		}

		if tuple != nil {
			tuples = append(tuples, tuple)
		}
	}

	return tuples
}

func (e *ExecutionEngine) createExecutor(plan plans.Plan, context *ExecutorContext) Executor {
	switch p := plan.(type) {
	case *plans.InsertPlanNode:
		return NewInsertExecutor(context, p)
	case *plans.SeqScanPlanNode:
		return NewSeqScanExecutor(context, p)
	case *plans.HashScanIndexPlanNode:
		return NewHashScanIndexExecutor(context, p)
	case *plans.LimitPlanNode:
		return NewLimitExecutor(context, p, e.createExecutor(plan.GetChildAt(0), context))
	}
	return nil
}

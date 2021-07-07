package executors

import (
	"github.com/brunocalza/go-bustub/execution/plans"
	"github.com/brunocalza/go-bustub/storage/table"
)

type ExecutionEngine struct {
}

func (e *ExecutionEngine) Execute(plan plans.Plan, context *ExecutorContext) []*table.Tuple {
	executor := e.createExecutor(plan, context)

	executor.Init()

	tuples := []*table.Tuple{}
	for {
		tuple, done, err := executor.Next()
		if err != nil || done {
			break
		}

		tuples = append(tuples, tuple)
	}

	return tuples
}

func (e *ExecutionEngine) createExecutor(plan plans.Plan, context *ExecutorContext) Executor {
	switch p := plan.(type) {
	case *plans.InsertPlanNode:
		return NewInsertExecutor(context, p)
	case *plans.SeqScanPlanNode:
		return NewSeqScanExecutor(context, p)
	}
	return nil
}

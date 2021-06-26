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

	tuples := make([]*table.Tuple, 0)
	tuple, _ := executor.Next()
	for tuple != nil {
		tuples = append(tuples, tuple)
		tuple, _ = executor.Next()
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

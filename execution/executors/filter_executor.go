package executors

import (
	"errors"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
)

// do filtering according to WHERE clause for Plan(Executor) which has no filtering feature

type FilterExecutor struct {
	context *ExecutorContext
	plan    *plans.FilterPlanNode // contains information about where clause
	child   Executor              // the child executor that will provide tuples to the this executor
}

func NewFilterExecutor(context *ExecutorContext, plan *plans.FilterPlanNode, child Executor) Executor {
	return &FilterExecutor{context, plan, child}
}

func (e *FilterExecutor) Init() {
	e.child.Init()
}

func (e *FilterExecutor) Next() (*tuple.Tuple, Done, error) {
	for t, done, err := e.child.Next(); !done; t, done, err = e.child.Next() {
		if err != nil {
			return nil, done, err
		}
		if t == nil && done == false {
			err := errors.New("e.child.Next returned nil unexpectedly.")
			return nil, true, err
		}

		if e.selects(t, e.plan.GetPredicate()) {
			return t, false, nil
		}
	}

	return nil, true, nil
}

func (e *FilterExecutor) GetOutputSchema() *schema.Schema {
	return e.plan.OutputSchema()
}

// select evaluates an expression on the tuple
func (e *FilterExecutor) selects(tuple *tuple.Tuple, predicate expression.Expression) bool {
	return predicate == nil || predicate.Evaluate(tuple, e.GetOutputSchema()).ToBoolean()
}

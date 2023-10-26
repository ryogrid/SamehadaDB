package executors

import (
	"errors"
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
)

// do filtering according to WHERE clause for Plan(Executor) which has no filtering feature

type SlectionExecutor struct {
	context *ExecutorContext
	plan    *plans.SelectionPlanNode // contains information about where clause
	child   Executor                 // the child executor that will provide tuples to the this executor
}

func NewSelectionExecutor(context *ExecutorContext, plan *plans.SelectionPlanNode, child Executor) Executor {
	return &SlectionExecutor{context, plan, child}
}

func (e *SlectionExecutor) Init() {
	e.child.Init()
}

func (e *SlectionExecutor) Next() (*tuple.Tuple, Done, error) {
	for t, done, err := e.child.Next(); !done; t, done, err = e.child.Next() {
		if err != nil {
			return nil, done, err
		}
		if t == nil && done == false {
			err := errors.New("e.child.Next returned nil unexpectedly.")
			e.context.txn.SetState(access.ABORTED)
			return nil, true, err
		}

		if !e.selects(t, e.plan.GetPredicate()) {
			continue
		}

		return t, false, nil
	}

	return nil, true, nil
}

func (e *SlectionExecutor) GetOutputSchema() *schema.Schema {
	return e.plan.OutputSchema()
}

// select evaluates an expression on the tuple
func (e *SlectionExecutor) selects(tuple *tuple.Tuple, predicate expression.Expression) bool {
	return predicate == nil || predicate.Evaluate(tuple, e.GetOutputSchema()).ToBoolean()
}

func (e *SlectionExecutor) GetTableMetaData() *catalog.TableMetadata {
	return e.child.GetTableMetaData()
}

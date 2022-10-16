package executors

import (
	"errors"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
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

		if !e.selects(t, e.plan.GetPredicate()) {
			continue
		}

		return e.projects(t), false, nil
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

// project applies the projection operator defined by the output schema
// It transform the tuple into a new tuple that corresponds to the output schema
func (e *FilterExecutor) projects(tuple_ *tuple.Tuple) *tuple.Tuple {
	srcOutSchema := e.plan.OutputSchema()
	filterSchema := e.plan.GetSelectColumns()

	values := []types.Value{}
	for i := uint32(0); i < filterSchema.GetColumnCount(); i++ {
		colIndex := srcOutSchema.GetColIndex(filterSchema.GetColumns()[i].GetColumnName())
		values = append(values, tuple_.GetValue(srcOutSchema, colIndex))
	}

	return tuple.NewTupleFromSchema(values, filterSchema)
}

func (e *FilterExecutor) GetTableMetaData() *catalog.TableMetadata { return e.child.GetTableMetaData() }

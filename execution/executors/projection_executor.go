package executors

import (
	"errors"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

type ProjectionExecutor struct {
	context *ExecutorContext
	plan    *plans.ProjectionPlanNode // contains information about where clause
	child   Executor                  // the child executor that will provide tuples to the this executor
}

func NewProjectionExecutor(context *ExecutorContext, plan *plans.ProjectionPlanNode, child Executor) Executor {
	return &ProjectionExecutor{context, plan, child}
}

func (e *ProjectionExecutor) Init() {
	e.child.Init()
}

func (e *ProjectionExecutor) Next() (*tuple.Tuple, Done, error) {
	for t, done, err := e.child.Next(); !done; t, done, err = e.child.Next() {
		if err != nil {
			return nil, done, err
		}
		if t == nil && done == false {
			err := errors.New("e.child.Next returned nil unexpectedly.")
			e.context.txn.SetState(access.ABORTED)
			return nil, true, err
		}

		return e.projects(t), false, nil
	}

	return nil, true, nil
}

func (e *ProjectionExecutor) GetOutputSchema() *schema.Schema {
	return e.plan.OutputSchema()
}

// project applies the projection operator defined by the output schema
func (e *ProjectionExecutor) projects(tuple_ *tuple.Tuple) *tuple.Tuple {
	srcOutSchema := e.plan.GetChildAt(0).OutputSchema()
	projectSchema := e.plan.OutputSchema()

	values := []types.Value{}
	for i := uint32(0); i < projectSchema.GetColumnCount(); i++ {
		colIndex := srcOutSchema.GetColIndex(projectSchema.GetColumns()[i].GetColumnName())
		values = append(values, tuple_.GetValue(srcOutSchema, colIndex))
	}

	return tuple.NewTupleFromSchema(values, projectSchema)
}

func (e *ProjectionExecutor) GetTableMetaData() *catalog.TableMetadata {
	return e.child.GetTableMetaData()
}

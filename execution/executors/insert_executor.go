package executors

import (
	"github.com/brunocalza/go-bustub/execution/plans"
	"github.com/brunocalza/go-bustub/storage/table"
)

// InsertExecutor executes an insert into a table
type InsertExecutor struct {
	context       *ExecutorContext
	plan          *plans.InsertPlanNode
	tableMetadata *table.TableMetadata
}

func NewInsertExecutor(context *ExecutorContext, plan *plans.InsertPlanNode) *InsertExecutor {
	tableMetadata := context.GetCatalog().GetTableByOID(plan.GetTableOID())
	return &InsertExecutor{context, plan, tableMetadata}
}

func (e *InsertExecutor) Init() {

}

// Next inserts the tuples into the tables
// Note that Insert does not return any tuple
// We return an error if the insert failed for any reason, and return nil if all inserts succeeded.
func (e *InsertExecutor) Next() (*table.Tuple, bool, error) {
	// let's assume it is raw insert

	for _, values := range e.plan.GetRawValues() {
		tuple := table.NewTupleFromSchema(values, e.tableMetadata.Schema())
		_, err := e.tableMetadata.Table().InsertTuple(tuple)
		if err != nil {
			return nil, true, err
		}
	}

	return nil, true, nil
}

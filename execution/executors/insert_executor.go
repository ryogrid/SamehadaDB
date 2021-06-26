package executors

import (
	"github.com/brunocalza/go-bustub/execution/plans"
	"github.com/brunocalza/go-bustub/storage/page"
	"github.com/brunocalza/go-bustub/storage/table"
)

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

func (e *InsertExecutor) Next() (*table.Tuple, *page.RID) {
	// let's assume it is raw insert
	for _, values := range e.plan.GetRawValues() {
		rid := &page.RID{}
		tuple := table.NewTupleFromSchema(values, e.tableMetadata.Schema())
		ok := e.tableMetadata.Table().InsertTuple(tuple, rid)
		if !ok {
			return nil, nil
		}
	}

	return nil, nil
}

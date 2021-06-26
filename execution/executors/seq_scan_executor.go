package executors

import (
	"github.com/brunocalza/go-bustub/execution"
	"github.com/brunocalza/go-bustub/execution/plans"
	"github.com/brunocalza/go-bustub/storage/page"
	"github.com/brunocalza/go-bustub/storage/table"
	"github.com/brunocalza/go-bustub/types"
)

type SeqScanExecutor struct {
	context        *ExecutorContext
	plan           *plans.SeqScanPlanNode
	tableMeatadata *table.TableMetadata
	predicate      *execution.Expression
	iterator       *table.TableIterator
}

func NewSeqScanExecutor(context *ExecutorContext, plan *plans.SeqScanPlanNode) *SeqScanExecutor {
	tableMetadata := context.GetCatalog().GetTableByOID(plan.GetTableOID())
	return &SeqScanExecutor{context, plan, tableMetadata, plan.GetPredicate(), nil}
}

func (e *SeqScanExecutor) Init() {
	e.iterator = e.tableMeatadata.Table().Begin()

}

func (e *SeqScanExecutor) Next() (tuple *table.Tuple, rid *page.RID) {
	currentTuple := e.iterator.Current()
	for currentTuple != nil {
		rid = currentTuple.GetRID()

		if e.predicate == nil || (*e.predicate).Evaluate(currentTuple, e.tableMeatadata.Schema()).(types.BooleanType).IsTrue() {
			outputSchema := e.plan.OutputSchema()
			columns := outputSchema.GetColumns()
			values := make([]types.Value, outputSchema.GetColumnCount())

			var i uint32
			for i = 0; i < uint32(len(values)); i++ {
				values[i] = currentTuple.GetValue(e.tableMeatadata.Schema(), uint32(e.tableMeatadata.Schema().GetColIndex(columns[i].GetColumnName())))
			}

			tuple := table.NewTupleFromSchema(values, outputSchema)
			e.iterator.Next()
			return tuple, rid
		} else {
			currentTuple = e.iterator.Next()
		}

	}
	return nil, nil
}

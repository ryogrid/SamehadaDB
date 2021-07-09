package executors

import (
	"github.com/brunocalza/go-bustub/execution/plans"
	"github.com/brunocalza/go-bustub/storage/table"
	"github.com/brunocalza/go-bustub/types"
)

type SeqScanExecutor struct {
	context        *ExecutorContext
	plan           *plans.SeqScanPlanNode
	tableMeatadata *table.TableMetadata
	iterator       *table.TableIterator
}

func NewSeqScanExecutor(context *ExecutorContext, plan *plans.SeqScanPlanNode) *SeqScanExecutor {
	tableMetadata := context.GetCatalog().GetTableByOID(plan.GetTableOID())
	return &SeqScanExecutor{context, plan, tableMetadata, nil}
}

func (e *SeqScanExecutor) Init() {
	e.iterator = e.tableMeatadata.Table().Begin()
}

func (e *SeqScanExecutor) Next() (*table.Tuple, bool, error) {
	currentTuple := e.iterator.Current()
	for currentTuple != nil {
		predicate := e.plan.GetPredicate()
		if predicate == nil || (*predicate).Evaluate(currentTuple, e.tableMeatadata.Schema()).ToBoolean() {
			outputSchema := e.plan.OutputSchema()
			columns := outputSchema.GetColumns()
			values := make([]types.Value, outputSchema.GetColumnCount())

			for i := uint32(0); i < uint32(len(values)); i++ {
				values[i] = currentTuple.GetValue(e.tableMeatadata.Schema(), uint32(e.tableMeatadata.Schema().GetColIndex(columns[i].GetColumnName())))
			}

			tuple := table.NewTupleFromSchema(values, outputSchema)
			e.iterator.Next()
			return tuple, false, nil
		} else {
			currentTuple = e.iterator.Next()
		}
	}
	return nil, true, nil
}

package executors

import (
	"github.com/brunocalza/go-bustub/catalog"
	"github.com/brunocalza/go-bustub/execution/expression"
	"github.com/brunocalza/go-bustub/execution/plans"
	"github.com/brunocalza/go-bustub/storage/access"
	"github.com/brunocalza/go-bustub/storage/table"
	"github.com/brunocalza/go-bustub/types"
)

// SeqScanExecutor executes a sequential scan
type SeqScanExecutor struct {
	context       *ExecutorContext
	plan          *plans.SeqScanPlanNode
	tableMetadata *catalog.TableMetadata
	it            *access.TableHeapIterator
}

// NewSeqScanExecutor creates a new sequential executor
func NewSeqScanExecutor(context *ExecutorContext, plan *plans.SeqScanPlanNode) Executor {
	tableMetadata := context.GetCatalog().GetTableByOID(plan.GetTableOID())
	return &SeqScanExecutor{context, plan, tableMetadata, nil}
}

func (e *SeqScanExecutor) Init() {
	e.it = e.tableMetadata.Table().Iterator()
}

// Next implements the next method for the sequential scan operator
// It uses the table heap iterator to iterate through the table heap
// tyring to find a tuple. It performs selection and projection on-the-fly
func (e *SeqScanExecutor) Next() (*table.Tuple, Done, error) {

	// iterates through the table heap trying to select a tuple that matches the predicate
	for t := e.it.Current(); !e.it.End(); t = e.it.Next() {
		if e.selects(t, e.plan.GetPredicate()) {
			break
		}
	}

	// if the iterator is not in the end, projects the current tuple into the output schema
	if !e.it.End() {
		defer e.it.Next() // advances the iterator after projection
		return e.projects(e.it.Current()), false, nil
	}

	return nil, true, nil
}

// select evaluates an expression on the tuple
func (e *SeqScanExecutor) selects(tuple *table.Tuple, predicate *expression.Expression) bool {
	return predicate == nil || (*predicate).Evaluate(tuple, e.tableMetadata.Schema()).ToBoolean()
}

// project applies the projection operator defined by the output schema
// It transform the tuple into a new tuple that corresponds to the output schema
func (e *SeqScanExecutor) projects(tuple *table.Tuple) *table.Tuple {
	outputSchema := e.plan.OutputSchema()

	values := []types.Value{}
	for i := uint32(0); i < outputSchema.GetColumnCount(); i++ {
		colIndex := e.tableMetadata.Schema().GetColIndex(outputSchema.GetColumns()[i].GetColumnName())
		values = append(values, tuple.GetValue(e.tableMetadata.Schema(), colIndex))
	}

	return table.NewTupleFromSchema(values, outputSchema)
}

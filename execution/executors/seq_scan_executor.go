// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package executors

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/interfaces"
	"github.com/ryogrid/SamehadaDB/storage/table"
	"github.com/ryogrid/SamehadaDB/types"
)

// SeqScanExecutor executes a sequential scan
type SeqScanExecutor struct {
	context       *ExecutorContext
	plan          *plans.SeqScanPlanNode
	tableMetadata *catalog.TableMetadata
	it            *interfaces.ITableHeapIterator
}

// NewSeqScanExecutor creates a new sequential executor
func NewSeqScanExecutor(context *ExecutorContext, plan *plans.SeqScanPlanNode) Executor {
	tableMetadata := context.GetCatalog().GetTableByOID(plan.GetTableOID())
	// TODO: (SDB) set LockManager and LogManager to Executor and TableHeap.
	//             reference can be get from Catalog class.
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

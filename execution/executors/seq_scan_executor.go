// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package executors

import (
	"fmt"

	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * SeqScanExecutor executes a sequential scan over a table.
 */
type SeqScanExecutor struct {
	context       *ExecutorContext
	plan          *plans.SeqScanPlanNode
	tableMetadata *catalog.TableMetadata
	it            *access.TableHeapIterator
	txn           *access.Transaction
}

// NewSeqScanExecutor creates a new sequential executor
func NewSeqScanExecutor(context *ExecutorContext, plan *plans.SeqScanPlanNode) Executor {
	tableMetadata := context.GetCatalog().GetTableByOID(plan.GetTableOID())
	//txn := access.NewTransaction(1)
	//catalog := context.GetCatalog()

	return &SeqScanExecutor{context, plan, tableMetadata, nil, context.GetTransaction()}
}

func (e *SeqScanExecutor) Init() {
	e.it = e.tableMetadata.Table().Iterator(e.txn)
}

// Next implements the next method for the sequential scan operator
// It uses the table heap iterator to iterate through the table heap
// tyring to find a tuple. It performs selection and projection on-the-fly
func (e *SeqScanExecutor) Next() (*tuple.Tuple, Done, error) {

	// iterates through the table heap trying to select a tuple that matches the predicate
	for t := e.it.Current(); !e.it.End(); t = e.it.Next() {
		fmt.Println(e.it.Current())
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
func (e *SeqScanExecutor) selects(tuple *tuple.Tuple, predicate *expression.Expression) bool {
	return predicate == nil || (*predicate).Evaluate(tuple, e.tableMetadata.Schema()).ToBoolean()
}

// project applies the projection operator defined by the output schema
// It transform the tuple into a new tuple that corresponds to the output schema
func (e *SeqScanExecutor) projects(tuple_ *tuple.Tuple) *tuple.Tuple {
	outputSchema := e.plan.OutputSchema()

	values := []types.Value{}
	for i := uint32(0); i < outputSchema.GetColumnCount(); i++ {
		colIndex := e.tableMetadata.Schema().GetColIndex(outputSchema.GetColumns()[i].GetColumnName())
		values = append(values, tuple_.GetValue(e.tableMetadata.Schema(), colIndex))
	}

	return tuple.NewTupleFromSchema(values, outputSchema)
}

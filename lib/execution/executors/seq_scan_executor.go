// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package executors

import (
	"errors"
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
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
		if t == nil {
			err := errors.New("e.it.Next returned nil")
			return nil, true, err
		}

		if e.selects(t, e.plan.GetPredicate()) {
			break
		}
	}

	// if the iterator is not in the end, projects the current tuple into the output schema
	if !e.it.End() {
		defer e.it.Next() // advances the iterator after projection
		ret := e.projects(e.it.Current())
		ret.SetRID(e.it.Current().GetRID())
		return ret, false, nil
	}

	return nil, true, nil
}

// select evaluates an expression on the tuple
func (e *SeqScanExecutor) selects(tuple *tuple.Tuple, predicate expression.Expression) bool {
	return predicate == nil || predicate.Evaluate(tuple, e.tableMetadata.Schema()).ToBoolean()
}

// project applies the projection operator defined by the output schema
func (e *SeqScanExecutor) projects(tuple_ *tuple.Tuple) *tuple.Tuple {
	outputSchema := e.plan.OutputSchema()

	values := []types.Value{}
	for i := uint32(0); i < outputSchema.GetColumnCount(); i++ {
		colName := outputSchema.GetColumns()[i].GetColumnName()

		colIndex := e.tableMetadata.Schema().GetColIndex(colName)
		values = append(values, tuple_.GetValue(e.tableMetadata.Schema(), colIndex))
	}

	return tuple.NewTupleFromSchema(values, outputSchema)
}

func (e *SeqScanExecutor) GetOutputSchema() *schema.Schema {
	return e.plan.OutputSchema()
}

func (e *SeqScanExecutor) GetTableMetaData() *catalog.TableMetadata {
	return e.tableMetadata
}

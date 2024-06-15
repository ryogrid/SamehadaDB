package executors

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/samehada/samehada_util"

	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

/**
 * PointScanWithIndexExecutor executes scan with hash index to filter rows matches predicate.
 */
type PointScanWithIndexExecutor struct {
	context       *ExecutorContext
	plan          *plans.PointScanWithIndexPlanNode
	tableMetadata *catalog.TableMetadata
	txn           *access.Transaction
	foundTuples   []*tuple.Tuple
}

func NewPointScanWithIndexExecutor(context *ExecutorContext, plan *plans.PointScanWithIndexPlanNode) Executor {
	tableMetadata := context.GetCatalog().GetTableByOID(plan.GetTableOID())

	return &PointScanWithIndexExecutor{context, plan, tableMetadata, context.GetTransaction(), make([]*tuple.Tuple, 0)}
}

func (e *PointScanWithIndexExecutor) Init() {
	comparison := e.plan.GetPredicate()
	schema_ := e.tableMetadata.Schema()
	colIdxOfPred := comparison.GetLeftSideColIdx()

	index_ := e.tableMetadata.GetIndex(int(colIdxOfPred))
	if index_ == nil {
		panic("PointScanWithIndexExecutor assumed index does not exist!.")
	}

	scanKey := samehada_util.GetPonterOfValue(comparison.GetRightSideValue(nil, schema_))
	dummyTuple := tuple.GenTupleForIndexSearch(schema_, colIdxOfPred, scanKey)
	rids := index_.ScanKey(dummyTuple, e.txn)
	if rids == nil || len(rids) == 0 {
		fmt.Println("any RID not found")
	}
	for _, rid := range rids {
		rid_ := rid
		tuple_, err := e.tableMetadata.Table().GetTuple(&rid_, e.txn)
		if tuple_ == nil && err != access.ErrSelfDeletedCase {
			//fmt.Println("PointScanWithIndexExecutor:Init ErrSelfDeletedCase!")
			e.foundTuples = make([]*tuple.Tuple, 0)
			e.txn.SetState(access.ABORTED)
			return
		}
		if err == access.ErrSelfDeletedCase {
			e.foundTuples = make([]*tuple.Tuple, 0)
			e.txn.SetState(access.ABORTED)
			return
		}
		if !tuple_.GetValue(schema_, colIdxOfPred).CompareEquals(*scanKey) {
			// found record is updated and commited case
			e.foundTuples = make([]*tuple.Tuple, 0)
			e.txn.SetState(access.ABORTED)
			return
		}
		e.foundTuples = append(e.foundTuples, tuple_)
	}
}

func (e *PointScanWithIndexExecutor) Next() (*tuple.Tuple, Done, error) {
	if e.txn.GetState() == access.ABORTED {
		return nil, true, access.ErrGeneral
	}

	if len(e.foundTuples) > 0 {
		tuple_ := e.foundTuples[0]
		e.foundTuples = e.foundTuples[1:]
		retTuple := e.projects(tuple_)
		retTuple.SetRID(tuple_.GetRID())
		return retTuple, false, nil
	}

	return nil, true, nil
}

// project applies the projection operator defined by the output schema
func (e *PointScanWithIndexExecutor) projects(tuple_ *tuple.Tuple) *tuple.Tuple {
	outputSchema := e.plan.OutputSchema()

	values := []types.Value{}
	for i := uint32(0); i < outputSchema.GetColumnCount(); i++ {
		colIndex := e.tableMetadata.Schema().GetColIndex(outputSchema.GetColumns()[i].GetColumnName())
		values = append(values, tuple_.GetValue(e.tableMetadata.Schema(), colIndex))
	}

	return tuple.NewTupleFromSchema(values, outputSchema)
}

func (e *PointScanWithIndexExecutor) GetOutputSchema() *schema.Schema {
	return e.plan.OutputSchema()
}

func (e *PointScanWithIndexExecutor) GetTableMetaData() *catalog.TableMetadata {
	return e.tableMetadata
}

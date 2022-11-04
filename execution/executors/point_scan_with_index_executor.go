package executors

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"

	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/index"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * PointScanWithIndexExecutor executes scan with hash index to filter rows matches predicate.
 */
type PointScanWithIndexExecutor struct {
	context       *ExecutorContext
	plan          *plans.PointScanWithIndexPlanNode
	tableMetadata *catalog.TableMetadata
	//it            *access.TableHeapIterator
	txn         *access.Transaction
	foundTuples []*tuple.Tuple
}

func NewPointScanWithIndexExecutor(context *ExecutorContext, plan *plans.PointScanWithIndexPlanNode) Executor {
	tableMetadata := context.GetCatalog().GetTableByOID(plan.GetTableOID())

	return &PointScanWithIndexExecutor{context, plan, tableMetadata, context.GetTransaction(), make([]*tuple.Tuple, 0)}
}

func (e *PointScanWithIndexExecutor) Init() {
	comparison := e.plan.GetPredicate()
	schema_ := e.tableMetadata.Schema()
	colIdxOfPred := comparison.GetLeftSideColIdx()

	colNum := int(e.tableMetadata.GetColumnNum())

	var index_ index.Index = nil
	var indexColNum int = -1
	for {
		index_ = nil
		for ii := indexColNum + 1; ii < colNum; ii++ {
			ret := e.tableMetadata.GetIndex(ii)
			if ret == nil {
				continue
			} else {
				index_ = ret
				indexColNum = ii
				break
			}
		}

		if index_ == nil || indexColNum == -1 {
			fmt.Printf("colIdxOfPred=%d,indexColNum=%d\n", colIdxOfPred, indexColNum)
			panic("PointScanWithIndexExecutor assumes that table which has index are passed.")
		}
		if colIdxOfPred != uint32(indexColNum) {
			// find next index having column
			continue
		}
		break
	}

	dummyTuple := tuple.GenTupleForIndexSearch(schema_, uint32(indexColNum), samehada_util.GetPonterOfValue(comparison.GetRightSideValue(nil, schema_)))
	rids := index_.ScanKey(dummyTuple, e.txn)
	for _, rid := range rids {
		tuple_ := e.tableMetadata.Table().GetTuple(&rid, e.txn)
		if tuple_ == nil {
			e.foundTuples = make([]*tuple.Tuple, 0)
			return
		}
		e.foundTuples = append(e.foundTuples, tuple_)
	}
}

func (e *PointScanWithIndexExecutor) Next() (*tuple.Tuple, Done, error) {
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
// It transform the tuple into a new tuple that corresponds to the output schema
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

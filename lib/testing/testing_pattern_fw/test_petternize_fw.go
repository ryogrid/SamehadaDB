package testing_pattern_fw

import (
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/execution/executors"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/column"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/testing/testing_assert"
	"github.com/ryogrid/SamehadaDB/lib/testing/testing_util"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"testing"
)

type Column struct {
	Name string
	Kind types.TypeID
}

type ColumnIdx struct {
	Name     string
	Kind     types.TypeID
	HasIndex bool
}

type Predicate struct {
	LeftColumn  string
	Operator    expression.ComparisonType
	RightColumn interface{}
}

type Assertion struct {
	Column string
	Exp    interface{}
}

type SeqScanTestCase struct {
	Description     string
	ExecutionEngine *executors.ExecutionEngine
	ExecutorContext *executors.ExecutorContext
	TableMetadata   *catalog.TableMetadata
	Columns         []Column
	Predicate       Predicate
	Asserts         []Assertion
	TotalHits       uint32
}

func ExecuteSeqScanTestCase(t *testing.T, testCase SeqScanTestCase) {
	columns := []*column.Column{}
	for _, c := range testCase.Columns {
		columns = append(columns, column.NewColumn(*testCase.TableMetadata.GetTableName()+"."+c.Name, c.Kind, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil))
	}
	outSchema := schema.NewSchema(columns)

	tmpColVal_ := expression.NewColumnValue(0, testCase.TableMetadata.Schema().GetColIndex(testCase.Predicate.LeftColumn), testing_util.GetValueType(testCase.Predicate.RightColumn))
	tmpColVal := tmpColVal_.(*expression.ColumnValue)
	expression := expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(testCase.Predicate.RightColumn), testing_util.GetValueType(testCase.Predicate.RightColumn)), testCase.Predicate.Operator, types.Boolean)
	seqScanPlan := plans.NewSeqScanPlanNode(testCase.ExecutorContext.GetCatalog(), outSchema, expression, testCase.TableMetadata.OID())

	results := testCase.ExecutionEngine.Execute(seqScanPlan, testCase.ExecutorContext)

	testing_assert.Equals(t, testCase.TotalHits, uint32(len(results)))
	if len(results) > 0 {
		for _, assert := range testCase.Asserts {
			colIndex := outSchema.GetColIndex(assert.Column)
			testing_assert.Assert(t, testing_util.GetValue(assert.Exp).CompareEquals(results[0].GetValue(outSchema, colIndex)), "value should be %v but was %v", assert.Exp, results[0].GetValue(outSchema, colIndex))
		}
	}
}

type IndexPointScanTestCase struct {
	Description     string
	ExecutionEngine *executors.ExecutionEngine
	ExecutorContext *executors.ExecutorContext
	TableMetadata   *catalog.TableMetadata
	Columns         []Column
	Predicate       Predicate
	Asserts         []Assertion
	TotalHits       uint32
}

type IndexRangeScanTestCase struct {
	Description     string
	ExecutionEngine *executors.ExecutionEngine
	ExecutorContext *executors.ExecutorContext
	TableMetadata   *catalog.TableMetadata
	Columns         []Column
	Predicate       Predicate
	ColIdx          int32 // column idx of column which has index to be used on scan
	ScanRange       []*types.Value
	TotalHits       uint32
}

func fillColumnsForIndexScanTestCase[T IndexPointScanTestCase | IndexRangeScanTestCase](testCase interface{}, indexType index_constants.IndexKind) []*column.Column {
	columns := []*column.Column{}

	var castedColumns []Column
	var tableName *string
	switch testCase.(type) {
	case IndexPointScanTestCase:
		castedColumns = testCase.(IndexPointScanTestCase).Columns
		tableName = testCase.(IndexPointScanTestCase).TableMetadata.GetTableName()
	case IndexRangeScanTestCase:
		castedColumns = testCase.(IndexRangeScanTestCase).Columns
		tableName = testCase.(IndexRangeScanTestCase).TableMetadata.GetTableName()
	}
	for _, c := range castedColumns {
		columns = append(columns, column.NewColumn(*tableName+"."+c.Name, c.Kind, true, indexType, types.PageID(-1), nil))
	}

	return columns
}

func ExecuteIndexPointScanTestCase(t *testing.T, testCase IndexPointScanTestCase, indexType index_constants.IndexKind) {
	columns := fillColumnsForIndexScanTestCase[IndexPointScanTestCase](testCase, indexType)
	outSchema := schema.NewSchema(columns)

	tmpColVal_ := expression.NewColumnValue(0, testCase.TableMetadata.Schema().GetColIndex(testCase.Predicate.LeftColumn), testing_util.GetValueType(testCase.Predicate.RightColumn))
	tmpColVal := tmpColVal_.(*expression.ColumnValue)

	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(testCase.Predicate.RightColumn), testing_util.GetValueType(testCase.Predicate.RightColumn)), testCase.Predicate.Operator, types.Boolean)
	hashIndexScanPlan := plans.NewPointScanWithIndexPlanNode(testCase.ExecutorContext.GetCatalog(), outSchema, expression_.(*expression.Comparison), testCase.TableMetadata.OID())

	results := testCase.ExecutionEngine.Execute(hashIndexScanPlan, testCase.ExecutorContext)

	testing_assert.Equals(t, testCase.TotalHits, uint32(len(results)))
	for _, assert := range testCase.Asserts {
		colIndex := outSchema.GetColIndex(assert.Column)
		testing_assert.Assert(t, testing_util.GetValue(assert.Exp).CompareEquals(results[0].GetValue(outSchema, colIndex)), "value should be %v but was %v", assert.Exp, results[0].GetValue(outSchema, colIndex))
	}
}

func ExecuteIndexRangeScanTestCase(t *testing.T, testCase IndexRangeScanTestCase, indexType index_constants.IndexKind) {
	columns := fillColumnsForIndexScanTestCase[IndexRangeScanTestCase](testCase, indexType)
	outSchema := schema.NewSchema(columns)

	tmpColVal_ := expression.NewColumnValue(0, testCase.TableMetadata.Schema().GetColIndex(testCase.Predicate.LeftColumn), testing_util.GetValueType(testCase.Predicate.RightColumn))
	tmpColVal := tmpColVal_.(*expression.ColumnValue)

	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(testCase.Predicate.RightColumn), testing_util.GetValueType(testCase.Predicate.RightColumn)), testCase.Predicate.Operator, types.Boolean)
	IndexRangeScanPlan := plans.NewRangeScanWithIndexPlanNode(testCase.ExecutorContext.GetCatalog(), outSchema, testCase.TableMetadata.OID(), testCase.ColIdx, expression_.(*expression.Comparison), testCase.ScanRange[0], testCase.ScanRange[1])

	results := testCase.ExecutionEngine.Execute(IndexRangeScanPlan, testCase.ExecutorContext)

	testing_assert.Equals(t, testCase.TotalHits, uint32(len(results)))
}

type DeleteTestCase struct {
	Description        string
	TransactionManager *access.TransactionManager
	ExecutionEngine    *executors.ExecutionEngine
	ExecutorContext    *executors.ExecutorContext
	TableMetadata      *catalog.TableMetadata
	Columns            []Column
	Predicate          Predicate
	Asserts            []Assertion
	TotalHits          uint32
}

func ExecuteDeleteTestCase(t *testing.T, testCase DeleteTestCase) {
	txn := testCase.TransactionManager.Begin(nil)
	// TODO: (SDB) avoiding crash... need to fix
	txn.SetIsRecoveryPhase(true)

	columns := []*column.Column{}
	for _, c := range testCase.Columns {
		columns = append(columns, column.NewColumn(*testCase.TableMetadata.GetTableName()+"."+c.Name, c.Kind, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil))
	}
	outSchema := schema.NewSchema(columns)

	tmpColVal_ := expression.NewColumnValue(0, testCase.TableMetadata.Schema().GetColIndex(testCase.Predicate.LeftColumn), testing_util.GetValueType(testCase.Predicate.RightColumn))
	tmpColVal := tmpColVal_.(*expression.ColumnValue)
	expression := expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(testCase.Predicate.RightColumn), testing_util.GetValueType(testCase.Predicate.RightColumn)), testCase.Predicate.Operator, types.Boolean)
	childSeqScanP := plans.NewSeqScanPlanNode(testCase.ExecutorContext.GetCatalog(), outSchema, expression, testCase.TableMetadata.OID())
	deletePlan := plans.NewDeletePlanNode(childSeqScanP)

	testCase.ExecutorContext.SetTransaction(txn)
	results := testCase.ExecutionEngine.Execute(deletePlan, testCase.ExecutorContext)

	testCase.TransactionManager.Commit(nil, txn)

	testing_assert.Equals(t, testCase.TotalHits, uint32(len(results)))
	for _, assert := range testCase.Asserts {
		colIndex := outSchema.GetColIndex(assert.Column)
		testing_assert.Assert(t, testing_util.GetValue(assert.Exp).CompareEquals(results[0].GetValue(outSchema, colIndex)), "value should be %v but was %v", assert.Exp, results[0].GetValue(outSchema, colIndex))
	}
}

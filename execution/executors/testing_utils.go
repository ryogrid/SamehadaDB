// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package executors

import (
	"github.com/ryogrid/SamehadaDB/storage/index/index_constants"
	"testing"

	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"

	testingpkg "github.com/ryogrid/SamehadaDB/testing"
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
	ExecutionEngine *ExecutionEngine
	ExecutorContext *ExecutorContext
	TableMetadata   *catalog.TableMetadata
	Columns         []Column
	Predicate       Predicate
	Asserts         []Assertion
	TotalHits       uint32
}

func ExecuteSeqScanTestCase(t *testing.T, testCase SeqScanTestCase) {
	columns := []*column.Column{}
	for _, c := range testCase.Columns {
		columns = append(columns, column.NewColumn(c.Name, c.Kind, false, index_constants.INDEX_KIND_INVAID, types.PageID(-1), nil))
	}
	outSchema := schema.NewSchema(columns)

	tmpColVal_ := expression.NewColumnValue(0, testCase.TableMetadata.Schema().GetColIndex(testCase.Predicate.LeftColumn), GetValueType(testCase.Predicate.RightColumn))
	tmpColVal := tmpColVal_.(*expression.ColumnValue)
	expression := expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(testCase.Predicate.RightColumn), GetValueType(testCase.Predicate.RightColumn)), testCase.Predicate.Operator, types.Boolean)
	seqPlan := plans.NewSeqScanPlanNode(outSchema, expression, testCase.TableMetadata.OID())

	results := testCase.ExecutionEngine.Execute(seqPlan, testCase.ExecutorContext)

	testingpkg.Equals(t, testCase.TotalHits, uint32(len(results)))
	if len(results) > 0 {
		for _, assert := range testCase.Asserts {
			colIndex := outSchema.GetColIndex(assert.Column)
			testingpkg.Assert(t, GetValue(assert.Exp).CompareEquals(results[0].GetValue(outSchema, colIndex)), "value should be %v but was %v", assert.Exp, results[0].GetValue(outSchema, colIndex))
		}
	}
}

type IndexPointScanTestCase struct {
	Description     string
	ExecutionEngine *ExecutionEngine
	ExecutorContext *ExecutorContext
	TableMetadata   *catalog.TableMetadata
	Columns         []Column
	Predicate       Predicate
	Asserts         []Assertion
	TotalHits       uint32
}

type IndexRangeScanTestCase struct {
	Description     string
	ExecutionEngine *ExecutionEngine
	ExecutorContext *ExecutorContext
	TableMetadata   *catalog.TableMetadata
	Columns         []Column
	Predicate       Predicate
	ColIdx          int32 // column idx of column which has index to be used on scan
	ScanRange       []types.Value
	TotalHits       uint32
}

func fillColumnsForIndexScanTestCase[T IndexPointScanTestCase | IndexRangeScanTestCase](testCase interface{}, indexType index_constants.IndexKind) []*column.Column {
	columns := []*column.Column{}

	var castedColumns []Column
	switch testCase.(type) {
	case IndexPointScanTestCase:
		castedColumns = testCase.(IndexPointScanTestCase).Columns
	case IndexRangeScanTestCase:
		castedColumns = testCase.(IndexRangeScanTestCase).Columns
	}
	for _, c := range castedColumns {
		columns = append(columns, column.NewColumn(c.Name, c.Kind, true, indexType, types.PageID(-1), nil))
	}

	return columns
}

func ExecuteIndexPointScanTestCase(t *testing.T, testCase IndexPointScanTestCase, indexType index_constants.IndexKind) {
	columns := fillColumnsForIndexScanTestCase[IndexPointScanTestCase](testCase, indexType)
	outSchema := schema.NewSchema(columns)

	tmpColVal_ := expression.NewColumnValue(0, testCase.TableMetadata.Schema().GetColIndex(testCase.Predicate.LeftColumn), GetValueType(testCase.Predicate.RightColumn))
	tmpColVal := tmpColVal_.(*expression.ColumnValue)

	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(testCase.Predicate.RightColumn), GetValueType(testCase.Predicate.RightColumn)), testCase.Predicate.Operator, types.Boolean)
	hashIndexScanPlan := plans.NewPointScanWithIndexPlanNode(outSchema, expression_.(*expression.Comparison), testCase.TableMetadata.OID())

	results := testCase.ExecutionEngine.Execute(hashIndexScanPlan, testCase.ExecutorContext)

	testingpkg.Equals(t, testCase.TotalHits, uint32(len(results)))
	for _, assert := range testCase.Asserts {
		colIndex := outSchema.GetColIndex(assert.Column)
		testingpkg.Assert(t, GetValue(assert.Exp).CompareEquals(results[0].GetValue(outSchema, colIndex)), "value should be %v but was %v", assert.Exp, results[0].GetValue(outSchema, colIndex))
	}
}

func ExecuteIndexRangeScanTestCase(t *testing.T, testCase IndexRangeScanTestCase, indexType index_constants.IndexKind) {
	columns := fillColumnsForIndexScanTestCase[IndexRangeScanTestCase](testCase, indexType)
	outSchema := schema.NewSchema(columns)

	tmpColVal_ := expression.NewColumnValue(0, testCase.TableMetadata.Schema().GetColIndex(testCase.Predicate.LeftColumn), GetValueType(testCase.Predicate.RightColumn))
	tmpColVal := tmpColVal_.(*expression.ColumnValue)

	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(testCase.Predicate.RightColumn), GetValueType(testCase.Predicate.RightColumn)), testCase.Predicate.Operator, types.Boolean)
	IndexRangeScanPlan := plans.NewRangeScanWithIndexPlanNode(outSchema, testCase.TableMetadata.OID(), testCase.ColIdx, expression_.(*expression.Comparison), &testCase.ScanRange[0], &testCase.ScanRange[1])

	results := testCase.ExecutionEngine.Execute(IndexRangeScanPlan, testCase.ExecutorContext)

	testingpkg.Equals(t, testCase.TotalHits, uint32(len(results)))
}

type DeleteTestCase struct {
	Description        string
	TransactionManager *access.TransactionManager
	ExecutionEngine    *ExecutionEngine
	ExecutorContext    *ExecutorContext
	TableMetadata      *catalog.TableMetadata
	Columns            []Column
	Predicate          Predicate
	Asserts            []Assertion
	TotalHits          uint32
}

func ExecuteDeleteTestCase(t *testing.T, testCase DeleteTestCase) {
	txn := testCase.TransactionManager.Begin(nil)

	columns := []*column.Column{}
	for _, c := range testCase.Columns {
		columns = append(columns, column.NewColumn(c.Name, c.Kind, false, index_constants.INDEX_KIND_INVAID, types.PageID(-1), nil))
	}
	outSchema := schema.NewSchema(columns)

	tmpColVal_ := expression.NewColumnValue(0, testCase.TableMetadata.Schema().GetColIndex(testCase.Predicate.LeftColumn), GetValueType(testCase.Predicate.RightColumn))
	tmpColVal := tmpColVal_.(*expression.ColumnValue)
	expression := expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(testCase.Predicate.RightColumn), GetValueType(testCase.Predicate.RightColumn)), testCase.Predicate.Operator, types.Boolean)
	deletePlan := plans.NewDeletePlanNode(expression, testCase.TableMetadata.OID())

	testCase.ExecutorContext.SetTransaction(txn)
	results := testCase.ExecutionEngine.Execute(deletePlan, testCase.ExecutorContext)

	testCase.TransactionManager.Commit(txn)

	testingpkg.Equals(t, testCase.TotalHits, uint32(len(results)))
	for _, assert := range testCase.Asserts {
		colIndex := outSchema.GetColIndex(assert.Column)
		testingpkg.Assert(t, GetValue(assert.Exp).CompareEquals(results[0].GetValue(outSchema, colIndex)), "value should be %v but was %v", assert.Exp, results[0].GetValue(outSchema, colIndex))
	}
}

func GetValue(data interface{}) (value types.Value) {
	switch v := data.(type) {
	case int:
		value = types.NewInteger(int32(v))
	case float32:
		value = types.NewFloat(float32(v))
	case string:
		value = types.NewVarchar(v)
	case bool:
		value = types.NewBoolean(v)
	case *types.Value:
		val := data.(*types.Value)
		return *val
	}
	return
}

func GetValueType(data interface{}) (value types.TypeID) {
	switch data.(type) {
	case int:
		return types.Integer
	case float32:
		return types.Float
	case string:
		return types.Varchar
	case bool:
		return types.Boolean
	case *types.Value:
		val := data.(*types.Value)
		return val.ValueType()
	}
	panic("not implemented")
}

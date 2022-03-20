// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package executors

import (
	"testing"

	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
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
		columns = append(columns, column.NewColumn(c.Name, c.Kind, false))
	}
	outSchema := schema.NewSchema(columns)

	expression := expression.NewComparison(expression.NewColumnValue(0, testCase.TableMetadata.Schema().GetColIndex(testCase.Predicate.LeftColumn)), expression.NewConstantValue(getValue(testCase.Predicate.RightColumn)), testCase.Predicate.Operator)
	seqPlan := plans.NewSeqScanPlanNode(outSchema, &expression, testCase.TableMetadata.OID())

	results := testCase.ExecutionEngine.Execute(seqPlan, testCase.ExecutorContext)

	testingpkg.Equals(t, testCase.TotalHits, uint32(len(results)))
	for _, assert := range testCase.Asserts {
		colIndex := outSchema.GetColIndex(assert.Column)
		testingpkg.Assert(t, getValue(assert.Exp).CompareEquals(results[0].GetValue(outSchema, colIndex)), "value should be %v but was %v", assert.Exp, results[0].GetValue(outSchema, colIndex))
	}
}

type HashIndexScanTestCase struct {
	Description     string
	ExecutionEngine *ExecutionEngine
	ExecutorContext *ExecutorContext
	TableMetadata   *catalog.TableMetadata
	Columns         []ColumnIdx
	Predicate       Predicate
	Asserts         []Assertion
	TotalHits       uint32
}

func ExecuteHashIndexScanTestCase(t *testing.T, testCase HashIndexScanTestCase) {
	columns := []*column.Column{}
	for _, c := range testCase.Columns {
		columns = append(columns, column.NewColumn(c.Name, c.Kind, c.HasIndex))
	}
	outSchema := schema.NewSchema(columns)

	expression := expression.NewComparisonAsComparison(expression.NewColumnValue(0, testCase.TableMetadata.Schema().GetColIndex(testCase.Predicate.LeftColumn)), expression.NewConstantValue(getValue(testCase.Predicate.RightColumn)), testCase.Predicate.Operator)
	//seqPlan := plans.NewSeqScanPlanNode(outSchema, &expression, testCase.TableMetadata.OID())
	hashIndexScanPlan := plans.NewHashScanIndexPlanNode(outSchema, expression, testCase.TableMetadata.OID())

	//results := testCase.ExecutionEngine.Execute(seqPlan, testCase.ExecutorContext)
	results := testCase.ExecutionEngine.Execute(hashIndexScanPlan, testCase.ExecutorContext)

	testingpkg.Equals(t, testCase.TotalHits, uint32(len(results)))
	for _, assert := range testCase.Asserts {
		colIndex := outSchema.GetColIndex(assert.Column)
		testingpkg.Assert(t, getValue(assert.Exp).CompareEquals(results[0].GetValue(outSchema, colIndex)), "value should be %v but was %v", assert.Exp, results[0].GetValue(outSchema, colIndex))
	}
}

func getValue(data interface{}) (value types.Value) {
	switch v := data.(type) {
	case int:
		value = types.NewInteger(int32(v))
	case string:
		value = types.NewVarchar(v)
	}
	return
}

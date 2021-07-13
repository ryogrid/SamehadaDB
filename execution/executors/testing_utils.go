package executors

import (
	"testing"

	"github.com/brunocalza/go-bustub/execution/expression"
	"github.com/brunocalza/go-bustub/execution/plans"
	"github.com/brunocalza/go-bustub/storage/table"
	"github.com/brunocalza/go-bustub/types"

	testingpkg "github.com/brunocalza/go-bustub/testing"
)

type Column struct {
	Name string
	Kind types.TypeID
}

type Predicate struct {
	LeftColumn  string
	Operator    expression.ComparisonType
	RightColumn types.TypeID
}

type Assertion struct {
	Column string
	Exp    types.TypeID
}

type SeqScanTestCase struct {
	Description     string
	ExecutionEngine *ExecutionEngine
	ExecutorContext *ExecutorContext
	TableMetadata   *table.TableMetadata
	Columns         []Column
	Predicate       Predicate
	Asserts         []Assertion
	TotalHits       uint32
}

func ExecuteSeqScanTestCase(t *testing.T, testCase SeqScanTestCase) {
	columns := []*table.Column{}
	for _, c := range testCase.Columns {
		columns = append(columns, table.NewColumn(c.Name, c.Kind))
	}
	outSchema := table.NewSchema(columns)

	expression := expression.NewComparison(expression.NewColumnValue(0, testCase.TableMetadata.Schema().GetColIndex(testCase.Predicate.LeftColumn)), expression.NewConstantValue(types.NewInteger(int32(testCase.Predicate.RightColumn))), testCase.Predicate.Operator)
	seqPlan := plans.NewSeqScanPlanNode(outSchema, &expression, testCase.TableMetadata.OID())

	results := testCase.ExecutionEngine.Execute(seqPlan, testCase.ExecutorContext)

	testingpkg.Equals(t, testCase.TotalHits, uint32(len(results)))
	for _, assert := range testCase.Asserts {
		colIndex := outSchema.GetColIndex(assert.Column)
		testingpkg.Assert(t, types.NewInteger(int32(assert.Exp)).CompareEquals(results[0].GetValue(outSchema, colIndex)), "value should be %d but was %d", int32(assert.Exp), results[0].GetValue(outSchema, colIndex).ToInteger())
	}
}

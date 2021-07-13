package executors

import (
	"testing"

	"github.com/brunocalza/go-bustub/execution/expression"
	"github.com/brunocalza/go-bustub/execution/plans"
	"github.com/brunocalza/go-bustub/storage/buffer"
	"github.com/brunocalza/go-bustub/storage/disk"
	"github.com/brunocalza/go-bustub/storage/table"
	testingpkg "github.com/brunocalza/go-bustub/testing"
	"github.com/brunocalza/go-bustub/types"
)

func TestSimpleInsertAndSeqScan(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager)

	c := table.NewCatalog(bpm)

	columnA := table.NewColumn("a", types.Integer)
	columnB := table.NewColumn("b", types.Integer)
	schema := table.NewSchema([]*table.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewInteger(22))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewInteger(55))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, bpm)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllpages()

	outColumnA := table.NewColumn("a", types.Integer)
	outSchema := table.NewSchema([]*table.Column{outColumnA})

	seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())

	results := executionEngine.Execute(seqPlan, executorContext)

	testingpkg.Assert(t, types.NewInteger(20).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 20")
	testingpkg.Assert(t, types.NewInteger(99).CompareEquals(results[1].GetValue(outSchema, 0)), "value should be 99")
}

func TestSimpleInsertAndSeqScanWithPredicateComparison(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager)

	c := table.NewCatalog(bpm)

	columnA := table.NewColumn("a", types.Integer)
	columnB := table.NewColumn("b", types.Integer)
	schema := table.NewSchema([]*table.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewInteger(22))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewInteger(55))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, bpm)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllpages()

	cases := []SeqScanTestCase{{
		"select a ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}},
		Predicate{"b", expression.Equal, 55},
		[]Assertion{{"a", 99}},
		1,
	}, {
		"select b ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"b", types.Integer}},
		Predicate{"b", expression.Equal, 55},
		[]Assertion{{"b", 55}},
		1,
	}, {
		"select a, b ... WHERE a = 20",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"a", expression.Equal, 20},
		[]Assertion{{"a", 20}, {"b", 22}},
		1,
	}, {
		"select a, b ... WHERE a = 99",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"a", expression.Equal, 99},
		[]Assertion{{"a", 99}, {"b", 55}},
		1,
	}, {
		"select a, b ... WHERE a = 100",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"a", expression.Equal, 100},
		[]Assertion{},
		0,
	}, {
		"select a, b ... WHERE b != 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"b", expression.NotEqual, 55},
		[]Assertion{{"a", 20}, {"b", 22}},
		1,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			ExecuteSeqScanTestCase(t, test)
		})
	}
}

func TestSimpleInsertAndLimitExecution(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager)

	c := table.NewCatalog(bpm)

	columnA := table.NewColumn("a", types.Integer)
	columnB := table.NewColumn("b", types.Integer)
	schema := table.NewSchema([]*table.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewInteger(22))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewInteger(55))

	row3 := make([]types.Value, 0)
	row3 = append(row3, types.NewInteger(11))
	row3 = append(row3, types.NewInteger(44))

	row4 := make([]types.Value, 0)
	row4 = append(row4, types.NewInteger(76))
	row4 = append(row4, types.NewInteger(90))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)
	rows = append(rows, row3)
	rows = append(rows, row4)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, bpm)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllpages()

	// TEST 1: select a, b ... LIMIT 1
	func() {
		a := table.NewColumn("a", types.Integer)
		b := table.NewColumn("b", types.Integer)
		outSchema := table.NewSchema([]*table.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 1, 1)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 1, len(results))
		testingpkg.Assert(t, types.NewInteger(99).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 99 but was %d", results[0].GetValue(outSchema, 0).ToInteger())
		testingpkg.Assert(t, types.NewInteger(55).CompareEquals(results[0].GetValue(outSchema, 1)), "value should be 55 but was %d", results[0].GetValue(outSchema, 1).ToInteger())
	}()

	// TEST 1: select a, b ... LIMIT 2
	func() {
		a := table.NewColumn("a", types.Integer)
		b := table.NewColumn("b", types.Integer)
		outSchema := table.NewSchema([]*table.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 2, 0)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 2, len(results))
	}()

	// TEST 1: select a, b ... LIMIT 3
	func() {
		a := table.NewColumn("a", types.Integer)
		b := table.NewColumn("b", types.Integer)
		outSchema := table.NewSchema([]*table.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 3, 0)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 3, len(results))
	}()

}

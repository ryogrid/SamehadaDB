package executors

import (
	"testing"

	"github.com/brunocalza/go-bustub/execution/expression"
	"github.com/brunocalza/go-bustub/execution/plans"
	"github.com/brunocalza/go-bustub/storage/buffer"
	"github.com/brunocalza/go-bustub/storage/disk"
	"github.com/brunocalza/go-bustub/storage/table"
	"github.com/brunocalza/go-bustub/testingutils"
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

	testingutils.Assert(t, types.NewInteger(20).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 20")
	testingutils.Assert(t, types.NewInteger(99).CompareEquals(results[1].GetValue(outSchema, 0)), "value should be 99")
}

func TestSimpleInsertAndSeqScanWithPredicateEqualsComparison(t *testing.T) {
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

	// TEST 1: select a ... WHERE b = 99
	func() {
		outColumn := table.NewColumn("a", types.Integer)
		outSchema := table.NewSchema([]*table.Column{outColumn})
		expression := expression.NewComparison(expression.NewColumnValue(0, 1), expression.NewConstantValue(types.NewInteger(55)), expression.Equal)
		seqPlan := plans.NewSeqScanPlanNode(outSchema, &expression, tableMetadata.OID())

		results := executionEngine.Execute(seqPlan, executorContext)

		testingutils.Equals(t, 1, len(results))
		testingutils.Assert(t, types.NewInteger(99).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 99 but was %d", results[0].GetValue(outSchema, 0).ToInteger())
	}()

	// TEST 2: select b ... WHERE b = 99
	func() {
		outColumn := table.NewColumn("b", types.Integer)
		outSchema := table.NewSchema([]*table.Column{outColumn})
		expression := expression.NewComparison(expression.NewColumnValue(0, 1), expression.NewConstantValue(types.NewInteger(55)), expression.Equal)
		seqPlan := plans.NewSeqScanPlanNode(outSchema, &expression, tableMetadata.OID())

		results := executionEngine.Execute(seqPlan, executorContext)

		testingutils.Equals(t, 1, len(results))
		testingutils.Assert(t, types.NewInteger(55).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 55 but was %d", results[0].GetValue(outSchema, 0).ToInteger())
	}()

	// TEST 3: select a, b ... WHERE a = 20
	func() {
		a := table.NewColumn("a", types.Integer)
		b := table.NewColumn("b", types.Integer)
		outSchema := table.NewSchema([]*table.Column{a, b})
		expression := expression.NewComparison(expression.NewColumnValue(0, 0), expression.NewConstantValue(types.NewInteger(20)), expression.Equal)
		seqPlan := plans.NewSeqScanPlanNode(outSchema, &expression, tableMetadata.OID())

		results := executionEngine.Execute(seqPlan, executorContext)

		testingutils.Equals(t, 1, len(results))
		testingutils.Assert(t, types.NewInteger(20).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 20 but was %d", results[0].GetValue(outSchema, 0).ToInteger())
		testingutils.Assert(t, types.NewInteger(22).CompareEquals(results[0].GetValue(outSchema, 1)), "value should be 22 but was %d", results[0].GetValue(outSchema, 1).ToInteger())
	}()

	// TEST 4: select a, b ... WHERE a = 99
	func() {
		a := table.NewColumn("a", types.Integer)
		b := table.NewColumn("b", types.Integer)
		outSchema := table.NewSchema([]*table.Column{a, b})
		expression := expression.NewComparison(expression.NewColumnValue(0, 0), expression.NewConstantValue(types.NewInteger(99)), expression.Equal)
		seqPlan := plans.NewSeqScanPlanNode(outSchema, &expression, tableMetadata.OID())

		results := executionEngine.Execute(seqPlan, executorContext)

		testingutils.Equals(t, 1, len(results))
		testingutils.Assert(t, types.NewInteger(99).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 99 but was %d", results[0].GetValue(outSchema, 0).ToInteger())
		testingutils.Assert(t, types.NewInteger(55).CompareEquals(results[0].GetValue(outSchema, 1)), "value should be 55 but was %d", results[0].GetValue(outSchema, 1).ToInteger())
	}()

	// TEST 5: select a, b ... WHERE a = 100
	func() {
		a := table.NewColumn("a", types.Integer)
		b := table.NewColumn("b", types.Integer)
		outSchema := table.NewSchema([]*table.Column{a, b})
		expression := expression.NewComparison(expression.NewColumnValue(0, 0), expression.NewConstantValue(types.NewInteger(100)), expression.Equal)
		seqPlan := plans.NewSeqScanPlanNode(outSchema, &expression, tableMetadata.OID())

		results := executionEngine.Execute(seqPlan, executorContext)
		testingutils.Equals(t, 0, len(results))
	}()
}

func TestSimpleInsertAndSeqScanWithPredicateNotEqualsComparison(t *testing.T) {
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

	// TEST 1: select a, b ... WHERE b != 55
	func() {
		a := table.NewColumn("a", types.Integer)
		b := table.NewColumn("b", types.Integer)
		outSchema := table.NewSchema([]*table.Column{a, b})
		expression := expression.NewComparison(expression.NewColumnValue(0, 1), expression.NewConstantValue(types.NewInteger(55)), expression.NotEqual)
		seqPlan := plans.NewSeqScanPlanNode(outSchema, &expression, tableMetadata.OID())

		results := executionEngine.Execute(seqPlan, executorContext)

		testingutils.Equals(t, 1, len(results))
		testingutils.Assert(t, types.NewInteger(20).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 20 but was %d", results[0].GetValue(outSchema, 0).ToInteger())
		testingutils.Assert(t, types.NewInteger(22).CompareEquals(results[0].GetValue(outSchema, 1)), "value should be 22 but was %d", results[0].GetValue(outSchema, 1).ToInteger())
	}()
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

		testingutils.Equals(t, 1, len(results))
		testingutils.Assert(t, types.NewInteger(99).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 99 but was %d", results[0].GetValue(outSchema, 0).ToInteger())
		testingutils.Assert(t, types.NewInteger(55).CompareEquals(results[0].GetValue(outSchema, 1)), "value should be 55 but was %d", results[0].GetValue(outSchema, 1).ToInteger())
	}()

	// TEST 1: select a, b ... LIMIT 2
	func() {
		a := table.NewColumn("a", types.Integer)
		b := table.NewColumn("b", types.Integer)
		outSchema := table.NewSchema([]*table.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 2, 0)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingutils.Equals(t, 2, len(results))
	}()

	// TEST 1: select a, b ... LIMIT 3
	func() {
		a := table.NewColumn("a", types.Integer)
		b := table.NewColumn("b", types.Integer)
		outSchema := table.NewSchema([]*table.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 3, 0)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingutils.Equals(t, 3, len(results))
	}()

}

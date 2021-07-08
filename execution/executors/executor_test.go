package executors

import (
	"testing"

	"github.com/brunocalza/go-bustub/execution/plans"
	"github.com/brunocalza/go-bustub/storage/buffer"
	"github.com/brunocalza/go-bustub/storage/disk"
	"github.com/brunocalza/go-bustub/storage/table"
	"github.com/brunocalza/go-bustub/testingutils"
	"github.com/brunocalza/go-bustub/types"
)

func TestInsertExecutor(t *testing.T) {
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

	outColumnB := table.NewColumn("a", types.Integer)
	outSchema := table.NewSchema([]*table.Column{outColumnB})

	seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID(), outSchema)

	results := executionEngine.Execute(seqPlan, executorContext)

	testingutils.Assert(t, types.NewInteger(20).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 20")
	testingutils.Assert(t, types.NewInteger(99).CompareEquals(results[1].GetValue(outSchema, 0)), "value should be 99")
}

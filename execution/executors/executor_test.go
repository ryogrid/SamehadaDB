package executors

import (
	"fmt"
	"testing"

	"github.com/brunocalza/go-bustub/buffer"
	"github.com/brunocalza/go-bustub/execution/plans"
	"github.com/brunocalza/go-bustub/storage/disk"
	"github.com/brunocalza/go-bustub/storage/table"
	"github.com/brunocalza/go-bustub/types"
)

func TestInsertExecutor(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	bpm := buffer.NewBufferPoolManager(diskManager, buffer.NewClockReplacer(10))

	c := table.NewCatalog(bpm)
	executorContext := NewExecutorContext(c, bpm)

	columnA := table.NewColumn("a", types.Integer)
	columnB := table.NewColumn("b", types.Integer)
	schema := table.NewSchema([]*table.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema)

	values1 := make([]types.Value, 0)
	values1 = append(values1, types.NewIntegerType(20))
	values1 = append(values1, types.NewIntegerType(22))

	values2 := make([]types.Value, 0)
	values2 = append(values2, types.NewIntegerType(99))
	values2 = append(values2, types.NewIntegerType(55))

	rawValues := make([][]types.Value, 0)
	rawValues = append(rawValues, values1)
	rawValues = append(rawValues, values2)

	insertPlanNode := plans.NewInsertPlanNode(rawValues, tableMetadata.OID())

	executionEngine := &ExecutionEngine{}
	executionEngine.Execute(insertPlanNode, executorContext)

	//bpm.FlushAllpages()

	outColumnB := table.NewColumn("b", types.Integer)
	outSchema := table.NewSchema([]*table.Column{outColumnB})

	seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID(), outSchema)

	results := executionEngine.Execute(seqPlan, executorContext)

	fmt.Println(results[0].GetValue(outSchema, 0))

	fmt.Println(results[1].GetValue(outSchema, 0))

	diskManager.ShutDown()
}

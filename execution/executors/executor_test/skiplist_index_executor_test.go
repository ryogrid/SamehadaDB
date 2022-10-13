package executor_test

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/execution/executors"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/disk"
	"github.com/ryogrid/SamehadaDB/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"testing"
)

func TestSkipListIndexPointScan(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true

	diskManager := disk.NewDiskManagerTest()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr) //, recovery.NewLogManager(diskManager), access.NewLockManager(access.REGULAR, access.PREVENTION))

	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	columnC := column.NewColumn("c", types.Varchar, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB, columnC})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewInteger(22))
	row1 = append(row1, types.NewVarchar("foo"))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewInteger(55))
	row2 = append(row2, types.NewVarchar("bar"))

	row3 := make([]types.Value, 0)
	row3 = append(row3, types.NewInteger(1225))
	row3 = append(row3, types.NewInteger(712))
	row3 = append(row3, types.NewVarchar("baz"))

	row4 := make([]types.Value, 0)
	row4 = append(row4, types.NewInteger(1226))
	row4 = append(row4, types.NewInteger(713))
	row4 = append(row4, types.NewVarchar("bazz"))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)
	rows = append(rows, row3)
	rows = append(rows, row4)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	txn_mgr.Commit(txn)

	cases := []executors.IndexPointScanTestCase{{
		"select a ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}},
		executors.Predicate{"b", expression.Equal, 55},
		[]executors.Assertion{{"a", 99}},
		1,
	}, {
		"select b ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"b", types.Integer}},
		executors.Predicate{"b", expression.Equal, 55},
		[]executors.Assertion{{"b", 55}},
		1,
	}, {
		"select a, b ... WHERE a = 20",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}, {"b", types.Integer}},
		executors.Predicate{"a", expression.Equal, 20},
		[]executors.Assertion{{"a", 20}, {"b", 22}},
		1,
	}, {
		"select a, b ... WHERE a = 99",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}, {"b", types.Integer}},
		executors.Predicate{"a", expression.Equal, 99},
		[]executors.Assertion{{"a", 99}, {"b", 55}},
		1,
	}, {
		"select a, b ... WHERE a = 100",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}, {"b", types.Integer}},
		executors.Predicate{"a", expression.Equal, 100},
		[]executors.Assertion{},
		0,
	}, {
		"select a, b ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}, {"b", types.Integer}},
		executors.Predicate{"b", expression.Equal, 55},
		[]executors.Assertion{{"a", 99}, {"b", 55}},
		1,
	}, {
		"select a, b, c ... WHERE c = 'foo'",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		executors.Predicate{"c", expression.Equal, "foo"},
		[]executors.Assertion{{"a", 20}, {"b", 22}, {"c", "foo"}},
		1,
	}, {
		"select a, b ... WHERE c = 'baz'",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}, {"b", types.Integer}},
		executors.Predicate{"c", expression.Equal, "baz"},
		[]executors.Assertion{{"a", 1225}, {"b", 712}},
		1,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			executors.ExecuteIndexPointScanTestCase(t, test, index_constants.INDEX_KIND_SKIP_LIST)
		})
	}

	common.TempSuppressOnMemStorage = false
	diskManager.ShutDown()
	common.TempSuppressOnMemStorageMutex.Unlock()
}

func TestSkipListSerialIndexRangeScan(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true

	diskManager := disk.NewDiskManagerTest()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr) //, recovery.NewLogManager(diskManager), access.NewLockManager(access.REGULAR, access.PREVENTION))

	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	columnC := column.NewColumn("c", types.Varchar, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB, columnC})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewInteger(22))
	row1 = append(row1, types.NewVarchar("foo"))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewInteger(55))
	row2 = append(row2, types.NewVarchar("bar"))

	row3 := make([]types.Value, 0)
	row3 = append(row3, types.NewInteger(1225))
	row3 = append(row3, types.NewInteger(712))
	row3 = append(row3, types.NewVarchar("baz"))

	row4 := make([]types.Value, 0)
	row4 = append(row4, types.NewInteger(1226))
	row4 = append(row4, types.NewInteger(713))
	row4 = append(row4, types.NewVarchar("bazz"))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)
	rows = append(rows, row3)
	rows = append(rows, row4)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	txn_mgr.Commit(txn)

	cases := []executors.IndexRangeScanTestCase{{
		"select a ... WHERE a >= 20 and a <= 1225",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}},
		executors.Predicate{"a", expression.GreaterThanOrEqual, 20},
		int32(tableMetadata.Schema().GetColIndex("a")),
		[]*types.Value{samehada_util.GetPonterOfValue(types.NewInteger(20)), samehada_util.GetPonterOfValue(types.NewInteger(1225))},
		3,
	}, {
		"select a ... WHERE a >= 20 and a <= 2147483647",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}},
		executors.Predicate{"a", expression.GreaterThanOrEqual, 20},
		int32(tableMetadata.Schema().GetColIndex("a")),
		[]*types.Value{samehada_util.GetPonterOfValue(types.NewInteger(20)), samehada_util.GetPonterOfValue(types.NewInteger(math.MaxInt32))},
		4,
	}, {
		"select a ... WHERE a >= -2147483646 and a <= 1225",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}},
		executors.Predicate{"a", expression.GreaterThanOrEqual, 20},
		int32(tableMetadata.Schema().GetColIndex("a")),
		[]*types.Value{samehada_util.GetPonterOfValue(types.NewInteger(math.MinInt32 + 1)), samehada_util.GetPonterOfValue(types.NewInteger(1225))},
		3,
	}, {
		"select a ... WHERE a >= -2147483646",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}},
		executors.Predicate{"a", expression.GreaterThanOrEqual, 20},
		int32(tableMetadata.Schema().GetColIndex("a")),
		[]*types.Value{samehada_util.GetPonterOfValue(types.NewInteger(math.MinInt32 + 1)), nil},
		4,
	}, {
		"select a ... WHERE a <= 1225",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}},
		executors.Predicate{"a", expression.GreaterThanOrEqual, 20},
		int32(tableMetadata.Schema().GetColIndex("a")),
		[]*types.Value{nil, samehada_util.GetPonterOfValue(types.NewInteger(1225))},
		3,
	}, {
		"select a ... ",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}},
		executors.Predicate{"a", expression.GreaterThanOrEqual, 20},
		int32(tableMetadata.Schema().GetColIndex("a")),
		[]*types.Value{nil, nil},
		4,
	}}
	/*, {
		"select a ... WHERE a >= -2147483647 and a <= 1225", // fail because restriction of current SkipList Index impl?
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}},
		executors.Predicate{"a", expression.GreaterThanOrEqual, 20},
		int32(tableMetadata.Schema().GetColIndex("a")),
		[]types.Value{types.NewInteger(math.MinInt32), types.NewInteger(1225)},
		3,
	}}
	*/

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			executors.ExecuteIndexRangeScanTestCase(t, test, index_constants.INDEX_KIND_SKIP_LIST)
		})
	}

	common.TempSuppressOnMemStorage = false
	diskManager.ShutDown()
	common.TempSuppressOnMemStorageMutex.Unlock()
}

/*
func getRandomPrimitiveVal[T int32 | float32 | string](keyType types.TypeID) T {
	switch keyType {
	case types.Integer:
		val := rand.Int31()
		if val < 0 {
			val = -1 * ((-1 * val) % (math.MaxInt32 >> 10))
		} else {
			val = val % (math.MaxInt32 >> 10)
		}
		var ret interface{} = val
		return ret.(T)
	case types.Float:
		var ret interface{} = rand.Float32()
		return ret.(T)
	case types.Varchar:
		//var ret interface{} = *samehada_util.GetRandomStr(1000)
		var ret interface{} = *samehada_util.GetRandomStr(500)
		//var ret interface{} = *samehada_util.GetRandomStr(50)
		return ret.(T)
	default:
		panic("not supported keyType")
	}
}

func choiceValFromMap[T int32 | float32 | string, V int32 | float32 | string](m map[T]V) T {
	l := len(m)
	i := 0

	index := rand.Intn(l)

	var ans T
	for k, _ := range m {
		if index == i {
			ans = k
			break
		} else {
			i++
		}
	}
	return ans
}

func rowInsertTransaction_(t *testing.T, shi *samehada.SamehadaInstance, c *catalog.Catalog, tm *catalog.TableMetadata, master_ch chan int32) {
	txn := shi.GetTransactionManager().Begin(nil)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewVarchar("hoge"))
	row1 = append(row1, types.NewInteger(40))
	row1 = append(row1, types.NewVarchar("hogehoge"))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewVarchar("foo"))
	row2 = append(row2, types.NewInteger(999))
	row2 = append(row2, types.NewVarchar("foofoo"))

	row3 := make([]types.Value, 0)
	row3 = append(row3, types.NewInteger(11))
	row3 = append(row3, types.NewVarchar("bar"))
	row3 = append(row3, types.NewInteger(17))
	row3 = append(row3, types.NewVarchar("barbar"))

	row4 := make([]types.Value, 0)
	row4 = append(row4, types.NewInteger(100))
	row4 = append(row4, types.NewVarchar("piyo"))
	row4 = append(row4, types.NewInteger(1000))
	row4 = append(row4, types.NewVarchar("piyopiyo"))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)
	rows = append(rows, row3)
	rows = append(rows, row4)

	insertPlanNode := plans.NewInsertPlanNode(rows, tm.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	ret := handleFnishTxn(shi.GetTransactionManager(), txn)
	master_ch <- ret
}

func deleteAllRowTransaction_(t *testing.T, shi *samehada.SamehadaInstance, c *catalog.Catalog, tm *catalog.TableMetadata, master_ch chan int32) {
	txn := shi.GetTransactionManager().Begin(nil)
	deletePlan := plans.NewDeletePlanNode(nil, tm.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)
	executionEngine.Execute(deletePlan, executorContext)

	ret := handleFnishTxn(shi.GetTransactionManager(), txn)
	master_ch <- ret
}

func selectAllRowTransaction_(t *testing.T, shi *samehada.SamehadaInstance, c *catalog.Catalog, tm *catalog.TableMetadata, master_ch chan int32) {
	txn := shi.GetTransactionManager().Begin(nil)

	outColumnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVAID, types.PageID(-1), nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnA})

	seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tm.OID())
	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	executionEngine.Execute(seqPlan, executorContext)

	ret := handleFnishTxn(shi.GetTransactionManager(), txn)
	master_ch <- ret
}

func TestConcurrentSkipListIndexUseTransactionExecution(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewVarchar("hoge"))
	row1 = append(row1, types.NewInteger(40))
	row1 = append(row1, types.NewVarchar("hogehoge"))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewVarchar("foo"))
	row2 = append(row2, types.NewInteger(999))
	row2 = append(row2, types.NewVarchar("foofoo"))

	row3 := make([]types.Value, 0)
	row3 = append(row3, types.NewInteger(11))
	row3 = append(row3, types.NewVarchar("bar"))
	row3 = append(row3, types.NewInteger(17))
	row3 = append(row3, types.NewVarchar("barbar"))

	row4 := make([]types.Value, 0)
	row4 = append(row4, types.NewInteger(100))
	row4 = append(row4, types.NewVarchar("piyo"))
	row4 = append(row4, types.NewInteger(1000))
	row4 = append(row4, types.NewVarchar("piyopiyo"))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)
	rows = append(rows, row3)
	rows = append(rows, row4)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	txn_mgr.Commit(txn)

	const PARALLEL_EXEC_CNT int = 100

	// // set timeout for debugging
	// time.AfterFunc(time.Duration(40)*time.Second, timeoutPanic)

	commited_cnt := int32(0)
	for i := 0; i < PARALLEL_EXEC_CNT; i++ {
		ch1 := make(chan int32)
		ch2 := make(chan int32)
		ch3 := make(chan int32)
		ch4 := make(chan int32)
		go rowInsertTransaction_(t, shi, c, tableMetadata, ch1)
		go selectAllRowTransaction_(t, shi, c, tableMetadata, ch2)
		go deleteAllRowTransaction_(t, shi, c, tableMetadata, ch3)
		go selectAllRowTransaction_(t, shi, c, tableMetadata, ch4)

		commited_cnt += <-ch1
		commited_cnt += <-ch2
		commited_cnt += <-ch3
		commited_cnt += <-ch4
		//fmt.Printf("commited_cnt: %d\n", commited_cnt)
		//shi.GetLockManager().PrintLockTables()
		//shi.GetLockManager().ClearLockTablesForDebug()
	}

}

func testSkipListMixParallelStrideAddedIterator[T int32 | float32 | string](t *testing.T, keyType types.TypeID, stride int32, opTimes int32, seedVal int32, initialEntryNum int32, bpoolSize int32) {
	common.ShPrintf(common.DEBUG_INFO, "start of testSkipListMixParallelStride stride=%d opTimes=%d seedVal=%d initialEntryNum=%d ====================================================\n",
		stride, opTimes, seedVal, initialEntryNum)

	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")

	txn_mgr := shi.GetTransactionManager()
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	columnC := column.NewColumn("c", types.Integer, false, index_constants.INDEX_KIND_INVAID, types.PageID(-1), nil)
	columnD := column.NewColumn("d", types.Varchar, false, index_constants.INDEX_KIND_INVAID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB, columnC, columnD})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	const THREAD_NUM = 20

	shi := samehada.NewSamehadaInstance(t.Name(), int(bpoolSize))
	//shi := samehada.NewSamehadaInstance(t.Name(), 30)
	//shi := samehada.NewSamehadaInstance(t.Name(), 60)

	//shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	//shi := samehada.NewSamehadaInstance(t.Name(), 10*1024) // buffer is about 40MB
	bpm := shi.GetBufferPoolManager()
	sl := skip_list.NewSkipList(bpm, keyType)

	checkDupMap := make(map[T]T)

	// override global rand seed (seed has been set on NewSkipList)
	//rand.Seed(3)
	rand.Seed(int64(seedVal))

	//tmpSkipRand := seedVal
	//// skip random value series
	//for tmpSkipRand > 0 {
	//	rand.Int31()
	//	tmpSkipRand--
	//}

	insVals := make([]T, 0)
	removedValsForGetAndRemove := make(map[T]T, 0)
	removedValsForRemove := make(map[T]T, 0)

	// initial entries
	useInitialEntryNum := int(initialEntryNum)
	for ii := 0; ii < useInitialEntryNum; ii++ {
		// avoid duplication
		insValBase := getRandomPrimitiveVal[T](keyType)
		for _, exist := checkDupMap[insValBase]; exist; _, exist = checkDupMap[insValBase] {
			insValBase = getRandomPrimitiveVal[T](keyType)
		}
		checkDupMap[insValBase] = insValBase

		for ii := int32(0); ii < stride; ii++ {
			insVal := strideAdd(strideMul(insValBase, stride), ii)
			pairVal := getValueForSkipListEntry(insVal)

			common.ShPrintf(common.DEBUGGING, "Insert op start.")
			sl.Insert(samehada_util.GetPonterOfValue(types.NewValue(insVal)), pairVal)
			//fmt.Printf("sl.Insert at insertRandom: ii=%d, insValBase=%d len(*insVals)=%d\n", ii, insValBase, len(insVals))
		}

		insVals = append(insVals, insValBase)
	}

	insValsMutex := new(sync.RWMutex)
	removedValsForGetMutex := new(sync.RWMutex)
	removedValsForRemoveMutex := new(sync.RWMutex)
	checkDupMapMutex := new(sync.RWMutex)

	ch := make(chan int32)

	useOpTimes := int(opTimes)
	runningThCnt := 0
	for ii := 0; ii <= useOpTimes; ii++ {
		// wait last go routines finishes
		if ii == useOpTimes {
			for runningThCnt > 0 {
				<-ch
				runningThCnt--
				common.ShPrintf(common.DEBUGGING, "runningThCnt=%d\n", runningThCnt)
			}
			break
		}

		// wait for keeping THREAD_NUM groroutine existing
		for runningThCnt >= THREAD_NUM {
			//for runningThCnt > 0 { // serial execution
			<-ch
			runningThCnt--

			common.ShPrintf(common.DEBUGGING, "runningThCnt=%d\n", runningThCnt)
		}
		common.ShPrintf(common.DEBUGGING, "ii=%d\n", ii)
		//runningThCnt = 0

		// get 0-4
		opType := rand.Intn(5)
		switch opType {
		case 0: // Insert
			go func() {
				//checkDupMapMutex.RLock()
				checkDupMapMutex.RLock()
				insValBase := getRandomPrimitiveVal[T](keyType)
				for _, exist := checkDupMap[insValBase]; exist; _, exist = checkDupMap[insValBase] {
					insValBase = getRandomPrimitiveVal[T](keyType)
				}
				checkDupMapMutex.RUnlock()
				checkDupMapMutex.Lock()
				checkDupMap[insValBase] = insValBase
				checkDupMapMutex.Unlock()

				for ii := int32(0); ii < stride; ii++ {
					insVal := strideAdd(strideMul(insValBase, stride), ii)
					pairVal := getValueForSkipListEntry(insVal)

					common.ShPrintf(common.DEBUGGING, "Insert op start.")
					sl.Insert(samehada_util.GetPonterOfValue(types.NewValue(insVal)), pairVal)
					//fmt.Printf("sl.Insert at insertRandom: ii=%d, insValBase=%d len(*insVals)=%d\n", ii, insValBase, len(insVals))
				}
				insValsMutex.Lock()
				insVals = append(insVals, insValBase)
				insValsMutex.Unlock()
				ch <- 1
			}()
		case 1, 2: // Delete
			// get 0-1 value
			tmpRand := rand.Intn(2)
			if tmpRand == 0 {
				// 50% is Remove to not existing entry
				go func() {
					removedValsForRemoveMutex.RLock()
					if len(removedValsForRemove) == 0 {
						removedValsForRemoveMutex.RUnlock()
						ch <- 1
						//continue
						return
					}
					removedValsForRemoveMutex.RUnlock()

					for ii := int32(0); ii < stride; ii++ {
						removedValsForRemoveMutex.RLock()
						delVal := choiceValFromMap(removedValsForRemove)
						removedValsForRemoveMutex.RUnlock()

						common.ShPrintf(common.DEBUGGING, "Remove(fail) op start.")
						isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(delVal)), getValueForSkipListEntry(delVal))
						common.SH_Assert(isDeleted == false, "delete should be fail!")
					}
					ch <- 1
				}()
			} else {
				// 50% is Remove to existing entry
				go func() {
					insValsMutex.Lock()
					if len(insVals)-1 < 0 {
						insValsMutex.Unlock()
						ch <- 1
						//continue
						return
					}
					tmpIdx := int(rand.Intn(len(insVals)))
					delValBase := insVals[tmpIdx]
					if len(insVals) == 1 {
						// make empty
						insVals = make([]T, 0)
					} else if len(insVals)-1 == tmpIdx {
						insVals = insVals[:len(insVals)-1]
					} else {
						insVals = append(insVals[:tmpIdx], insVals[tmpIdx+1:]...)
					}
					insValsMutex.Unlock()

					for ii := int32(0); ii < stride; ii++ {
						delVal := strideAdd(strideMul(delValBase, stride), ii).(T)
						pairVal := getValueForSkipListEntry(delVal)
						common.ShPrintf(common.DEBUGGING, "Remove(success) op start.")

						// append to map before doing remove op for other get op thread
						removedValsForGetMutex.Lock()
						removedValsForGetAndRemove[delVal] = delVal
						removedValsForGetMutex.Unlock()

						isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(delVal)), pairVal)
						if isDeleted == true {
							// append to map after doing remove op for other fail remove op thread
							removedValsForRemoveMutex.Lock()
							removedValsForRemove[delVal] = delVal
							removedValsForRemoveMutex.Unlock()

						} else {
							removedValsForGetMutex.RLock()
							if _, ok := removedValsForGetAndRemove[delVal]; !ok {
								removedValsForGetMutex.RUnlock()
								panic("remove op test failed!")
							}
							removedValsForGetMutex.RUnlock()
							//panic("remove op test failed!")
						}
					}
					ch <- 1
					//common.SH_Assert(isDeleted == true, "remove should be success!")
				}()
			}
		case 3: // Get
			go func() {
				insValsMutex.RLock()
				if len(insVals) == 0 {
					insValsMutex.RUnlock()
					ch <- 1
					//continue
					return
				}
				tmpIdx := int(rand.Intn(len(insVals)))
				//fmt.Printf("sl.GetValue at testSkipListMix: ii=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*removedValsForGetAndRemove)=%d\n", ii, tmpIdx, insVals[tmpIdx], len(insVals), len(removedValsForGetAndRemove))
				getTgtBase := insVals[tmpIdx]
				insValsMutex.RUnlock()
				for ii := int32(0); ii < stride; ii++ {
					getTgt := strideAdd(strideMul(getTgtBase, stride), ii).(T)
					getTgtVal := types.NewValue(getTgt)
					correctVal := getValueForSkipListEntry(getTgt)

					common.ShPrintf(common.DEBUGGING, "Get op start.")
					gotVal := sl.GetValue(&getTgtVal)
					if gotVal == math.MaxUint32 {
						removedValsForGetMutex.RLock()
						if _, ok := removedValsForGetAndRemove[getTgt]; !ok {
							removedValsForGetMutex.RUnlock()
							panic("get op test failed!")
						}
						removedValsForGetMutex.RUnlock()
					} else if gotVal != correctVal {
						panic("returned value of get of is wrong!")
					}
				}
				ch <- 1
				//common.SH_Assert(, "gotVal is not collect!")
			}()
		case 4: //GetRangeScanIterator
			go func() {
				insValsMutex.RLock()
				if len(insVals) == 0 {
					insValsMutex.RUnlock()
					ch <- 1
					//continue
					return
				}
				tmpIdx := int(rand.Intn(len(insVals)))
				//fmt.Printf("sl.GetValue at testSkipListMix: ii=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*removedValsForGetAndRemove)=%d\n", ii, tmpIdx, insVals[tmpIdx], len(insVals), len(removedValsForGetAndRemove))
				rangeStartBase := insVals[tmpIdx]
				insValsMutex.RUnlock()
				rangeStartVal := types.NewValue(rangeStartBase)
				rangeEndBase := strideAdd(rangeStartBase, stride).(T)
				rangeEndVal := types.NewValue(rangeEndBase)
				itr := sl.Iterator(&rangeStartVal, &rangeEndVal, nil)
				for done, _, _, _ := itr.Next(); !done; done, _, _, _ = itr.Next() {
				}

				ch <- 1
			}()
		}
		runningThCnt++
	}
	shi.CloseFilesForTesting()
}
*/

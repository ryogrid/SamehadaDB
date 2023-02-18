package executor_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/container/hash"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/samehada"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

func testKeyDuplicateInsertDeleteWithSkipListIndex[T float32 | int32 | string](t *testing.T, keyType types.TypeID) {
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), 500)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")
	txnMgr := shi.GetTransactionManager()

	txn := txnMgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)

	columnA := column.NewColumn("account_id", keyType, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	columnB := column.NewColumn("balance", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})
	tableMetadata := c.CreateTable("test_1", schema_, txn)

	txnMgr.Commit(c, txn)

	txn = txnMgr.Begin(nil)

	var accountId interface{}
	switch keyType {
	case types.Integer:
		accountId = int32(10)
	case types.Float:
		accountId = float32(-5.2)
	case types.Varchar:
		accountId = "duplicateTest"
	default:
		panic("unsuppoted value type")
	}

	insPlan1 := createSpecifiedValInsertPlanNode(accountId.(T), int32(100), c, tableMetadata, keyType)
	result := executePlan(c, shi.GetBufferPoolManager(), txn, insPlan1)
	insPlan2 := createSpecifiedValInsertPlanNode(accountId.(T), int32(101), c, tableMetadata, keyType)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, insPlan2)
	insPlan3 := createSpecifiedValInsertPlanNode(accountId.(T), int32(102), c, tableMetadata, keyType)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, insPlan3)

	txnMgr.Commit(c, txn)

	txn = txnMgr.Begin(nil)

	scanP := createSpecifiedPointScanPlanNode(accountId.(T), c, tableMetadata, keyType, index_constants.INDEX_KIND_SKIP_LIST)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, scanP)
	testingpkg.Assert(t, len(result) == 3, "duplicated key point scan got illegal results.")
	rid1 := result[0].GetRID()
	val0_1 := result[0].GetValue(tableMetadata.Schema(), 0)
	val0_2 := result[0].GetValue(tableMetadata.Schema(), 1)
	fmt.Println(val0_1, val0_2)
	rid2 := result[1].GetRID()
	rid3 := result[2].GetRID()
	fmt.Printf("%v %v %v\n", *rid1, *rid2, *rid3)

	indexCol1 := tableMetadata.GetIndex(0)
	indexCol2 := tableMetadata.GetIndex(1)

	indexCol1.DeleteEntry(result[0], *rid1, txn)
	indexCol2.DeleteEntry(result[0], *rid1, txn)
	scanP = createSpecifiedPointScanPlanNode(accountId.(T), c, tableMetadata, keyType, index_constants.INDEX_KIND_SKIP_LIST)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, scanP)
	testingpkg.Assert(t, len(result) == 2, "duplicated key point scan got illegal results.")

	indexCol1.DeleteEntry(result[0], *rid2, txn)
	indexCol2.DeleteEntry(result[0], *rid2, txn)
	scanP = createSpecifiedPointScanPlanNode(accountId.(T), c, tableMetadata, keyType, index_constants.INDEX_KIND_SKIP_LIST)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, scanP)
	testingpkg.Assert(t, len(result) == 1, "duplicated key point scan got illegal results.")

	indexCol1.DeleteEntry(result[0], *rid3, txn)
	indexCol2.DeleteEntry(result[0], *rid3, txn)
	scanP = createSpecifiedPointScanPlanNode(accountId.(T), c, tableMetadata, keyType, index_constants.INDEX_KIND_SKIP_LIST)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, scanP)
	testingpkg.Assert(t, len(result) == 0, "duplicated key point scan got illegal results.")

	txnMgr.Commit(c, txn)
}

// maxVal is *int32 for int32 and float32
func getNotDupWithAccountRandomPrimitivVal[T int32 | float32 | string](keyType types.TypeID, checkDupMap map[T]T, checkDupMapMutex *sync.RWMutex, maxVal interface{}) T {
	checkDupMapMutex.Lock()
	retVal := samehada_util.GetRandomPrimitiveVal[T](keyType, maxVal)
	for _, exist := checkDupMap[retVal]; exist; _, exist = checkDupMap[retVal] {
		retVal = samehada_util.GetRandomPrimitiveVal[T](keyType, maxVal)
	}
	checkDupMap[retVal] = retVal
	checkDupMapMutex.Unlock()
	return retVal
}

func testParallelTxnsQueryingSkipListIndexUsedColumns[T int32 | float32 | string](t *testing.T, keyType types.TypeID, stride int32, opTimes int32, seedVal int32, initialEntryNum int32, bpoolSize int32, indexKind index_constants.IndexKind, execType int32, threadNum int) {
	common.ShPrintf(common.DEBUG_INFO, "start of testParallelTxnsQueryingUniqSkipListIndexUsedColumns stride=%d opTimes=%d seedVal=%d initialEntryNum=%d bpoolSize=%d ====================================================\n",
		stride, opTimes, seedVal, initialEntryNum, bpoolSize)

	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), int(bpoolSize))
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")

	txnMgr := shi.GetTransactionManager()
	txn := txnMgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)

	var columnA *column.Column
	var columnB *column.Column
	switch indexKind {
	case index_constants.INDEX_KIND_INVALID:
		columnA = column.NewColumn("account_id", keyType, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		columnB = column.NewColumn("balance", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	case index_constants.INDEX_KIND_SKIP_LIST:
		columnA = column.NewColumn("account_id", keyType, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
		columnB = column.NewColumn("balance", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	default:
		panic("not implemented!")
	}
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)
	txnMgr.Commit(nil, txn)

	// ignored when execType is SERIAL_EXEC
	//const THREAD_NUM = 20 //1 // 10 //20 // 2

	rand.Seed(int64(seedVal))

	insVals := make([]T, 0)
	insValsMutex := new(sync.RWMutex)
	deletedValsForDelete := make(map[T]T, 0)
	deletedValsForDeleteMutex := new(sync.RWMutex)
	checkKeyColDupMap := make(map[T]T)
	checkKeyColDupMapMutex := new(sync.RWMutex)

	insertedTupleCnt := int32(0)
	deletedTupleCnt := int32(0)
	executedTxnCnt := int32(0)
	abortedTxnCnt := int32(0)
	commitedTxnCnt := int32(0)

	//// TODO: for debugging
	//balanceAmountForRandom := int32(100000)
	//balanceAmountForRandomMutex := new(sync.Mutex)

	txn = txnMgr.Begin(nil)

	// insert account records
	const ACCOUNT_NUM = 10 //20 //4
	const BALANCE_AT_START = 1000
	sumOfAllAccountBalanceAtStart := int32(0)
	accountIds := make([]T, 0)
	for ii := 0; ii < ACCOUNT_NUM; ii++ {
		accountId := samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
		for _, exist := checkKeyColDupMap[accountId]; exist; _, exist = checkKeyColDupMap[accountId] {
			accountId = samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
		}
		checkKeyColDupMap[accountId] = accountId
		accountIds = append(accountIds, accountId)
		// not have to duplication check of barance
		insPlan := createSpecifiedValInsertPlanNode(accountId, int32(BALANCE_AT_START+ii), c, tableMetadata, keyType)
		executePlan(c, shi.GetBufferPoolManager(), txn, insPlan)
		sumOfAllAccountBalanceAtStart += int32(BALANCE_AT_START + ii)
	}
	txnMgr.Commit(nil, txn)

	txn = txnMgr.Begin(nil)

	insertedTupleCnt += ACCOUNT_NUM

	handleFnishedTxn := func(catalog_ *catalog.Catalog, txn_mgr *access.TransactionManager, txn *access.Transaction) bool {
		//// TODO: for debugging
		//var randVal int
		//randVal = rand.Intn(3)
		//if randVal == 0 || randVal == 1 {
		//	txn.SetState(access.ABORTED)
		//}

		// fmt.Println(txn.GetState())
		if txn.GetState() == access.ABORTED {
			// fmt.Println(txn.GetSharedLockSet())
			// fmt.Println(txn.GetExclusiveLockSet())
			txn_mgr.Abort(catalog_, txn)
			return false
		} else {
			// fmt.Println(txn.GetSharedLockSet())
			// fmt.Println(txn.GetExclusiveLockSet())
			txn_mgr.Commit(catalog_, txn)
			return true
		}
	}

	getInt32ValCorrespondToPassVal := func(val interface{}) int32 {
		switch val.(type) {
		case int32:
			return val.(int32)
		case float32:
			return int32(val.(float32))
		case string:
			casted := val.(string)
			byteArr := make([]byte, len(casted))
			copy(byteArr, casted)
			//return int32(hash.GenHashMurMur(byteArr)) % (math.MaxInt32 / stride)
			return int32(hash.GenHashMurMur(byteArr)) //% (math.MaxInt32 / stride)
		default:
			panic("unsupported type!")
		}
	}

	// setup other initial entries which is not used as account
	useInitialEntryNum := int(initialEntryNum)
	for ii := 0; ii < useInitialEntryNum; ii++ {
		keyValBase := getNotDupWithAccountRandomPrimitivVal[T](keyType, checkKeyColDupMap, checkKeyColDupMapMutex, nil)

		for ii := int32(0); ii < stride; ii++ {
			insKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(keyValBase, stride), ii).(T)
			insBranceVal := getInt32ValCorrespondToPassVal(insKeyVal)

			insPlan := createSpecifiedValInsertPlanNode(insKeyVal, insBranceVal, c, tableMetadata, keyType)
			executePlan(c, shi.GetBufferPoolManager(), txn, insPlan)
		}

		insVals = append(insVals, keyValBase)
	}

	insertedTupleCnt += initialEntryNum * stride

	txnMgr.Commit(nil, txn)

	ch := make(chan int32)

	abortTxnAndUpdateCounter := func(txn_ *access.Transaction) {
		handleFnishedTxn(c, txnMgr, txn_)
		atomic.AddInt32(&executedTxnCnt, 1)
		atomic.AddInt32(&abortedTxnCnt, 1)
		if execType == PARALLEL_EXEC {
			ch <- 1
		}
	}

	insValsAppendWithLock := func(keyVal T) {
		insValsMutex.Lock()
		insVals = append(insVals, keyVal)
		insValsMutex.Unlock()
	}

	checkKeyColDupMapDeleteWithLock := func(keyVal T) {
		checkKeyColDupMapMutex.Lock()
		delete(checkKeyColDupMap, keyVal)
		checkKeyColDupMapMutex.Unlock()
	}

	deleteCheckMapEntriesWithLock := func(keyValBase T) {
		checkKeyColDupMapDeleteWithLock(keyValBase)
	}

	finalizeRandomNoSideEffectTxn := func(txn_ *access.Transaction) {
		txnOk := handleFnishedTxn(c, txnMgr, txn_)

		if txnOk {
			atomic.AddInt32(&commitedTxnCnt, 1)
		} else {
			atomic.AddInt32(&abortedTxnCnt, 1)
		}
		atomic.AddInt32(&executedTxnCnt, 1)
	}

	// internally, issue new transaction
	checkTotalBalanceNoChange := func() {
		txn_ := txnMgr.Begin(nil)
		txn_.SetDebugInfo("checkTotalBrance-Op")
		sumOfAllAccountBalanceAfterTest := int32(0)
		for ii := 0; ii < ACCOUNT_NUM; ii++ {
			selPlan := createSpecifiedPointScanPlanNode(accountIds[ii], c, tableMetadata, keyType, indexKind)
			results := executePlan(c, shi.GetBufferPoolManager(), txn_, selPlan)
			//common.SH_Assert(txn_.GetState() != access.ABORTED, "txn state should not be ABORTED!")
			if txn_.GetState() == access.ABORTED {
				handleFnishedTxn(c, txnMgr, txn_)
				return
			}
			common.SH_Assert(results != nil && len(results) == 1, fmt.Sprintf("point scan result count is not 1 (%d)!\n", len(results)))
			sumOfAllAccountBalanceAfterTest += results[0].GetValue(tableMetadata.Schema(), 1).ToInteger()
		}
		common.SH_Assert(sumOfAllAccountBalanceAfterTest == sumOfAllAccountBalanceAtStart, fmt.Sprintf("total account volume is changed! %d != %d\n", sumOfAllAccountBalanceAfterTest, sumOfAllAccountBalanceAtStart))
		finalizeRandomNoSideEffectTxn(txn_)
	}

	finalizeAccountUpdateTxn := func(txn_ *access.Transaction) {
		//if rand.Intn(3) == 0 {
		//	txn_.SetState(access.ABORTED)
		//}

		txnOk := handleFnishedTxn(c, txnMgr, txn_)

		if txnOk {
			checkTotalBalanceNoChange()
			atomic.AddInt32(&commitedTxnCnt, 1)
		} else {
			checkTotalBalanceNoChange()
			atomic.AddInt32(&abortedTxnCnt, 1)
		}
		atomic.AddInt32(&executedTxnCnt, 1)
	}

	finalizeRandomInsertTxn := func(txn_ *access.Transaction, insKeyValBase T) {
		txnOk := handleFnishedTxn(c, txnMgr, txn_)
		if txnOk {
			insValsAppendWithLock(insKeyValBase)
			//putCheckMapEntriesWithLock(insKeyValBase)
			atomic.AddInt32(&insertedTupleCnt, stride)
			atomic.AddInt32(&commitedTxnCnt, 1)
		} else {
			// rollback
			deleteCheckMapEntriesWithLock(insKeyValBase)
			atomic.AddInt32(&abortedTxnCnt, 1)
		}
		atomic.AddInt32(&executedTxnCnt, 1)
	}

	finalizeRandomDeleteTxn := func(txn_ *access.Transaction, delKeyValBase T) {
		txnOk := handleFnishedTxn(c, txnMgr, txn_)
		if txnOk {
			deletedValsForDeleteMutex.Lock()
			deletedValsForDelete[delKeyValBase] = delKeyValBase
			deletedValsForDeleteMutex.Unlock()
			deleteCheckMapEntriesWithLock(delKeyValBase)
			atomic.AddInt32(&deletedTupleCnt, stride)
			atomic.AddInt32(&commitedTxnCnt, 1)
		} else {
			// rollback removed element
			insValsAppendWithLock(delKeyValBase)
			atomic.AddInt32(&abortedTxnCnt, 1)
		}
		atomic.AddInt32(&executedTxnCnt, 1)
	}

	finalizeRandomUpdateTxn := func(txn_ *access.Transaction, oldKeyValBase T, newKeyValBase T) {
		txnOk := handleFnishedTxn(c, txnMgr, txn_)
		if txnOk {
			// append new base value
			insValsAppendWithLock(newKeyValBase)
			deleteCheckMapEntriesWithLock(oldKeyValBase)
			//deleteCheckMapEntriesWithLock(newKeyValBase)
			atomic.AddInt32(&commitedTxnCnt, 1)
		} else {
			// rollback removed element
			insValsAppendWithLock(oldKeyValBase)
			deleteCheckMapEntriesWithLock(newKeyValBase)
			atomic.AddInt32(&abortedTxnCnt, 1)
		}
		atomic.AddInt32(&executedTxnCnt, 1)
	}

	useOpTimes := int(opTimes)
	runningThCnt := 0
	for ii := 0; ii <= useOpTimes; ii++ {
		// wait last go routines finishes
		if ii == useOpTimes && execType == PARALLEL_EXEC {
			for runningThCnt > 0 {
				<-ch
				runningThCnt--
				common.ShPrintf(common.DEBUGGING, "runningThCnt=%d\n", runningThCnt)
			}
			break
		}

		// wait for keeping THREAD_NUM groroutine existing
		for runningThCnt >= threadNum && execType == PARALLEL_EXEC {
			//for runningThCnt > 0 { // serial execution
			<-ch
			runningThCnt--

			common.ShPrintf(common.DEBUGGING, "runningThCnt=%d\n", runningThCnt)
		}
		common.ShPrintf(common.DEBUGGING, "ii=%d\n", ii)

		// get 0-7
		opType := rand.Intn(8)

		//// move money, random Insert, Delete
		//opType := rand.Intn(4)

		//// move money, Random Insert, Randome Delete, Random Update
		//opType := rand.Intn(5)

		//// move money, Random Insert, Randome Delete, Random Update, Random Point Scan
		//opType := rand.Intn(7)

		//// Random Insert only
		//opType := 1

		//// Random Isert and Random Delete only
		//opType := rand.Intn(3)
		//opType += 1

		//// move money, Random Insert, Randome Delete, Random Update, Random Point Scan, Random Range Scan
		//opType := rand.Intn(8)

		//// move money, Random Insert, Randome Delete, (Random Update), Random Point Scan, Random Range Scan
		//opType := rand.Intn(7)
		//if opType >= 4 {
		//	opType = opType + 1
		//}

		//// (move money), Random Insert, Randome Delete, (Random Update), Random Point Scan, Random Range Scan
		//opType := rand.Intn(6)
		//opType += 1
		//if opType >= 4 {
		//	opType = opType + 1
		//}

		//// move money, Random Insert, (Randome Delete), Random Update, Random Point Scan, Random Range Scan
		//opType := rand.Intn(7)
		//if opType >= 2 {
		//	opType = opType + 1
		//}

		//// move money, Random Insert, Randome Delete, ,Random Point Scan, Random Range Scan
		//opType := rand.Intn(7)
		//if opType >= 4 {
		//	opType += 1
		//}

		//// move money only
		//opType := 0

		//// move money, Random Insert, Random Update, Random Point Scan
		//opType := rand.Intn(5)
		//if opType >= 2 {
		//	opType = opType + 2
		//}
		switch opType {
		case 0: // Update two account balance (move money)
			moveMoneyOpFunc := func() {
				txn_ := txnMgr.Begin(nil)
				txn_.SetDebugInfo("MoneyMove-Op")

				// decide accounts
				idx1 := rand.Intn(ACCOUNT_NUM)
				idx2 := idx1 + 1
				if idx2 == ACCOUNT_NUM {
					idx2 = 0
				}

				// get current volume of money move accounts
				selPlan1 := createSpecifiedPointScanPlanNode(accountIds[idx1], c, tableMetadata, keyType, indexKind)
				results1 := executePlan(c, shi.GetBufferPoolManager(), txn_, selPlan1)
				if txn_.GetState() == access.ABORTED {
					abortTxnAndUpdateCounter(txn_)
					return
				}
				if results1 == nil || len(results1) != 1 {
					//time.Sleep(time.Second * 120)
					panic("balance check failed(1).")
				}
				balance1 := results1[0].GetValue(tableMetadata.Schema(), 1).ToInteger()

				selPlan2 := createSpecifiedPointScanPlanNode(accountIds[idx2], c, tableMetadata, keyType, indexKind)
				results2 := executePlan(c, shi.GetBufferPoolManager(), txn_, selPlan2)
				if txn_.GetState() == access.ABORTED {
					abortTxnAndUpdateCounter(txn_)
					return
				}
				if results2 == nil || len(results2) != 1 {
					//time.Sleep(time.Second * 120)
					panic("balance check failed(2).")
				}
				balance2 := results2[0].GetValue(tableMetadata.Schema(), 1).ToInteger()

				// decide move ammount

				var newBalance1 int32
				var newBalance2 int32
				if balance1 > balance2 {
				retry1_1:
					newBalance1 = samehada_util.GetRandomPrimitiveVal[int32](types.Integer, &balance1)
					newBalance2 = balance2 + (balance1 - newBalance1)

					if newBalance1 <= 0 || newBalance2 <= 0 {
						goto retry1_1
					}
				} else {
				retry1_2:
					newBalance2 = samehada_util.GetRandomPrimitiveVal[int32](types.Integer, &balance2)
					newBalance1 = balance1 + (balance2 - newBalance2)

					if newBalance1 <= 0 || newBalance2 <= 0 {
						goto retry1_2
					}
				}

				// create plans and execute these

				common.ShPrintf(common.DEBUGGING, "Update account op start.\n")

				if newBalance1 > sumOfAllAccountBalanceAtStart || newBalance1 < 0 {
					fmt.Printf("money move op: newBalance1 is broken. %d\n", newBalance1)
				}
				updatePlan1 := createBalanceUpdatePlanNode(accountIds[idx1], newBalance1, c, tableMetadata, keyType, indexKind)
				updateRslt1 := executePlan(c, shi.GetBufferPoolManager(), txn_, updatePlan1)

				if txn_.GetState() == access.ABORTED {
					finalizeAccountUpdateTxn(txn_)
					if execType == PARALLEL_EXEC {
						ch <- 1
					}
					return
				}

				common.SH_Assert(len(updateRslt1) == 1 && txn_.GetState() != access.ABORTED, fmt.Sprintf("account update fails!(1) txn_.txn_id:%v", txn_.GetTransactionId()))

				if newBalance2 > sumOfAllAccountBalanceAtStart || newBalance2 < 0 {
					fmt.Printf("money move op: newBalance2 is broken. %d\n", newBalance2)
				}
				updatePlan2 := createBalanceUpdatePlanNode(accountIds[idx2], newBalance2, c, tableMetadata, keyType, indexKind)
				updateRslt2 := executePlan(c, shi.GetBufferPoolManager(), txn_, updatePlan2)

				if txn_.GetState() == access.ABORTED {
					finalizeAccountUpdateTxn(txn_)
					if execType == PARALLEL_EXEC {
						ch <- 1
					}
					return
				}

				common.SH_Assert(len(updateRslt2) == 1 && txn_.GetState() != access.ABORTED, fmt.Sprintf("account update fails!(2) txn_.txn_id:%v", txn_.GetTransactionId()))

				finalizeAccountUpdateTxn(txn_)
				if execType == PARALLEL_EXEC {
					ch <- 1
				}
			}
			if execType == PARALLEL_EXEC {
				go moveMoneyOpFunc()
			} else {
				moveMoneyOpFunc()
			}
		case 1: // Insert
			randomInsertOpFunc := func() {
				var tmpMax interface{}
				if keyType == types.Float {
					tmpMax = math.MaxFloat32 / float32(stride)
				} else {
					tmpMax = math.MaxInt32 / stride
				}

				insKeyValBase := getNotDupWithAccountRandomPrimitivVal[T](keyType, checkKeyColDupMap, checkKeyColDupMapMutex, tmpMax)

				txn_ := txnMgr.Begin(nil)

				txn_.SetDebugInfo("Insert(random)-Op")
				common.ShPrintf(common.DEBUGGING, fmt.Sprintf("Insert op start. txnId:%v ii:%d\n", txn_.GetTransactionId(), ii))

				for jj := int32(0); jj < stride; jj++ {
					insKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(insKeyValBase, stride), jj).(T)
					insBalanceVal := getInt32ValCorrespondToPassVal(insKeyVal)

					common.ShPrintf(common.DEBUGGING, fmt.Sprintf("Insert op start. txnId:%v ii:%d jj:%d\n", txn_.GetTransactionId(), ii, jj))
					insPlan := createSpecifiedValInsertPlanNode(insKeyVal, insBalanceVal, c, tableMetadata, keyType)
					executePlan(c, shi.GetBufferPoolManager(), txn_, insPlan)

					if txn_.GetState() == access.ABORTED {
						break
					}
					//fmt.Printf("sl.Insert at insertRandom: jj=%d, insKeyValBase=%d len(*insVals)=%d\n", jj, insKeyValBase, len(insVals))
				}

				finalizeRandomInsertTxn(txn_, insKeyValBase)
				if execType == PARALLEL_EXEC {
					ch <- 1
				}
			}
			if execType == PARALLEL_EXEC {
				go randomInsertOpFunc()
			} else {
				randomInsertOpFunc()
			}
		case 2, 3: // Delete
			// get 0-1 value
			tmpRand := rand.Intn(2)
			//tmpRand := 1
			if tmpRand == 0 {
				// 50% is Delete to not existing entry
				randomDeleteFailOpFunc := func() {
					deletedValsForDeleteMutex.RLock()
					if len(deletedValsForDelete) == 0 {
						deletedValsForDeleteMutex.RUnlock()
						if execType == PARALLEL_EXEC {
							ch <- 1
						}
						return
					}
					deletedValsForDeleteMutex.RUnlock()

					txn_ := txnMgr.Begin(nil)

					txn_.SetDebugInfo("Delete(fail)-Op")
					deletedValsForDeleteMutex.RLock()
					delKeyValBase := samehada_util.ChoiceValFromMap(deletedValsForDelete)
					deletedValsForDeleteMutex.RUnlock()
					for jj := int32(0); jj < stride; jj++ {
						delKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(delKeyValBase, stride), jj).(T)

						common.ShPrintf(common.DEBUGGING, "Delete(fail) op start.\n")
						delPlan := createSpecifiedValDeletePlanNode(delKeyVal, c, tableMetadata, keyType, indexKind)
						results := executePlan(c, shi.GetBufferPoolManager(), txn_, delPlan)

						if txn_.GetState() == access.ABORTED {
							break
						}

						common.SH_Assert(results != nil && len(results) == 0, "delete(fail) should not be fail!")
					}

					finalizeRandomNoSideEffectTxn(txn_)

					if execType == PARALLEL_EXEC {
						ch <- 1
					}
				}
				if execType == PARALLEL_EXEC {
					go randomDeleteFailOpFunc()
				} else {
					randomDeleteFailOpFunc()
				}
			} else {
				// 50% is Delete to existing entry
				randomDeleteSuccessOpFunc := func() {
					insValsMutex.Lock()
					if len(insVals)-1 < 0 {
						insValsMutex.Unlock()
						if execType == PARALLEL_EXEC {
							ch <- 1
						}
						return
					}
					tmpIdx := int(rand.Intn(len(insVals)))
					delKeyValBase := insVals[tmpIdx]
					if len(insVals) == 1 {
						// make empty
						insVals = make([]T, 0)
					} else if len(insVals)-1 == tmpIdx {
						insVals = insVals[:len(insVals)-1]
					} else {
						insVals = append(insVals[:tmpIdx], insVals[tmpIdx+1:]...)
					}
					insValsMutex.Unlock()

					txn_ := txnMgr.Begin(nil)

					txn_.SetDebugInfo("Delete(success)-Op")

					for jj := int32(0); jj < stride; jj++ {
						delKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(delKeyValBase, stride), jj).(T)

						common.ShPrintf(common.DEBUGGING, "Delete(success) op start %v.\n", delKeyVal)

						delPlan := createSpecifiedValDeletePlanNode(delKeyVal, c, tableMetadata, keyType, indexKind)
						results := executePlan(c, shi.GetBufferPoolManager(), txn_, delPlan)

						if txn_.GetState() == access.ABORTED {
							break
						}

						common.SH_Assert(results != nil && len(results) == 1, "Delete(success) failed!")
					}

					finalizeRandomDeleteTxn(txn_, delKeyValBase)
					if execType == PARALLEL_EXEC {
						ch <- 1
					}
				}
				if execType == PARALLEL_EXEC {
					go randomDeleteSuccessOpFunc()
				} else {
					randomDeleteSuccessOpFunc()
				}
			}
		case 4: // Random Update
			randomUpdateOpFunc := func() {
				insValsMutex.Lock()
				if len(insVals) == 0 {
					insValsMutex.Unlock()
					if execType == PARALLEL_EXEC {
						ch <- 1
					}
					return
				}
				tmpIdx := int(rand.Intn(len(insVals)))
				updateKeyValBase := insVals[tmpIdx]
				if len(insVals) == 1 {
					// make empty
					insVals = make([]T, 0)
				} else if len(insVals)-1 == tmpIdx {
					insVals = insVals[:len(insVals)-1]
				} else {
					insVals = append(insVals[:tmpIdx], insVals[tmpIdx+1:]...)
				}
				insValsMutex.Unlock()

				var tmpMax interface{}
				if keyType == types.Float {
					tmpMax = math.MaxFloat32 / float32(stride)
				} else {
					tmpMax = math.MaxInt32 / stride
				}
				updateNewKeyValBase := getNotDupWithAccountRandomPrimitivVal[T](keyType, checkKeyColDupMap, checkKeyColDupMapMutex, &tmpMax)

				txn_ := txnMgr.Begin(nil)
				txn_.SetDebugInfo("Update(random)-Op")

				for jj := int32(0); jj < stride; jj++ {
					updateKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(updateKeyValBase, stride), jj).(T)
					updateNewKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(updateNewKeyValBase, stride), jj).(T)
					newBalanceVal := getInt32ValCorrespondToPassVal(updateNewKeyVal)

					common.ShPrintf(common.DEBUGGING, "Update (random) op start.")

					updatePlan1 := createAccountIdUpdatePlanNode(updateKeyVal, updateNewKeyVal, c, tableMetadata, keyType, indexKind)
					results1 := executePlan(c, shi.GetBufferPoolManager(), txn_, updatePlan1)

					if txn_.GetState() == access.ABORTED {
						break
					}

					common.SH_Assert(results1 != nil && len(results1) == 1, "Update failed!")

					updatePlan2 := createBalanceUpdatePlanNode(updateNewKeyVal, newBalanceVal, c, tableMetadata, keyType, indexKind)

					results2 := executePlan(c, shi.GetBufferPoolManager(), txn_, updatePlan2)

					if txn_.GetState() == access.ABORTED {
						break
					}

					common.SH_Assert(results2 != nil && len(results2) == 1, "Update failed!")
				}

				finalizeRandomUpdateTxn(txn_, updateKeyValBase, updateNewKeyValBase)
				if execType == PARALLEL_EXEC {
					ch <- 1
				}
			}
			if execType == PARALLEL_EXEC {
				go randomUpdateOpFunc()
			} else {
				randomUpdateOpFunc()
			}
		case 5, 6: // Select (Point Scan)
			// get 0-1 value
			tmpRand := rand.Intn(2)
			if tmpRand == 0 { // 50% is Select to not existing entry
				randomPointScanFailOpFunc := func() {
					deletedValsForDeleteMutex.RLock()
					if len(deletedValsForDelete) == 0 {
						deletedValsForDeleteMutex.RUnlock()
						if execType == PARALLEL_EXEC {
							ch <- 1
						}
						return
					}
					getTgtBase := samehada_util.ChoiceValFromMap(deletedValsForDelete)
					deletedValsForDeleteMutex.RUnlock()
					txn_ := txnMgr.Begin(nil)
					txn_.SetDebugInfo("Select(point|fail)-Op")
					for jj := int32(0); jj < stride; jj++ {
						getKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(getTgtBase, stride), jj).(T)

						common.ShPrintf(common.DEBUGGING, "Select(fail) op start.")
						selectPlan := createSpecifiedPointScanPlanNode(getKeyVal, c, tableMetadata, keyType, indexKind)
						results := executePlan(c, shi.GetBufferPoolManager(), txn_, selectPlan)

						if txn_.GetState() == access.ABORTED {
							break
						}

						common.SH_Assert(results != nil && len(results) == 0, "Select(fail) should be fail!")
					}
					finalizeRandomNoSideEffectTxn(txn_)
					if execType == PARALLEL_EXEC {
						ch <- 1
					}
				}
				if execType == PARALLEL_EXEC {
					go randomPointScanFailOpFunc()
				} else {
					randomPointScanFailOpFunc()
				}
			} else { // 50% is Select to existing entry
				randomPointScanSuccessOpFunc := func() {
					insValsMutex.RLock()
					if len(insVals) == 0 {
						insValsMutex.RUnlock()
						if execType == PARALLEL_EXEC {
							ch <- 1
						}
						return
					}
					tmpIdx := int(rand.Intn(len(insVals)))
					//fmt.Printf("sl.GetValue at testSkipListMix: jj=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*deletedValsForSelectUpdate)=%d\n", jj, tmpIdx, insVals[tmpIdx], len(insVals), len(deletedValsForSelectUpdate))
					getKeyValBase := insVals[tmpIdx]
					insValsMutex.RUnlock()
					txn_ := txnMgr.Begin(nil)
					txn_.SetDebugInfo("Select(point|success)-Op")
					for jj := int32(0); jj < stride; jj++ {
						getKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(getKeyValBase, stride), jj).(T)

						common.ShPrintf(common.DEBUGGING, "Select(success) op start.")
						selectPlan := createSpecifiedPointScanPlanNode(getKeyVal, c, tableMetadata, keyType, indexKind)
						results := executePlan(c, shi.GetBufferPoolManager(), txn_, selectPlan)

						if txn_.GetState() == access.ABORTED {
							break
						}

						//common.SH_Assert(results != nil && len(results) == 1, "Select(success) should not be fail!")
						collectVal := types.NewInteger(getInt32ValCorrespondToPassVal(getKeyVal))
						gotVal := results[0].GetValue(tableMetadata.Schema(), 1)
						common.SH_Assert(gotVal.CompareEquals(collectVal), "value should be "+fmt.Sprintf("%d not %d", collectVal.ToInteger(), gotVal.ToInteger()))
					}
					finalizeRandomNoSideEffectTxn(txn_)
					if execType == PARALLEL_EXEC {
						ch <- 1
					}
				}
				if execType == PARALLEL_EXEC {
					go randomPointScanSuccessOpFunc()
				} else {
					randomPointScanSuccessOpFunc()
				}
			}
		case 7: // Select (Range Scan)
			randomRangeScanOpFunc := func() {
				insValsMutex.RLock()
				if len(insVals) < 2 {
					insValsMutex.RUnlock()
					if execType == PARALLEL_EXEC {
						ch <- 1
					}
					return
				}
				common.ShPrintf(common.DEBUGGING, "Select(success) op start.\n")
				tmpIdx1 := int(rand.Intn(len(insVals)))
				tmpIdx2 := int(rand.Intn(len(insVals)))
				diffToMakeNoExist := int32(10)
				rangeStartKey := insVals[tmpIdx1]
				rangeEndKey := insVals[tmpIdx2]
				insValsMutex.RUnlock()
				// get 0-8 value
				tmpRand := rand.Intn(9)
				var rangeScanPlan plans.Plan
				switch tmpRand {
				case 0: // start only
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, &rangeStartKey, nil, indexKind)
				case 1: // end only
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, nil, &rangeEndKey, indexKind)
				case 2: // start and end
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, &rangeStartKey, &rangeEndKey, indexKind)
				case 3: // not specified both
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, nil, nil, indexKind)
				case 4: // start only (not exisiting val)
					tmpStartKey := samehada_util.StrideAdd(rangeStartKey, diffToMakeNoExist).(T)
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, &tmpStartKey, nil, indexKind)
				case 5: // end only (not existing val)
					tmpEndKey := samehada_util.StrideAdd(rangeEndKey, diffToMakeNoExist).(T)
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, nil, &tmpEndKey, indexKind)
				case 6: // start and end (start val is not existing one)
					tmpStartKey := samehada_util.StrideAdd(rangeStartKey, diffToMakeNoExist).(T)
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, &tmpStartKey, &rangeEndKey, indexKind)
				case 7: // start and end (start val is not existing one)
					tmpEndKey := samehada_util.StrideAdd(rangeEndKey, diffToMakeNoExist).(T)
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, &rangeStartKey, &tmpEndKey, indexKind)
				case 8: // start and end (end val is not existing one)
					tmpStartKey := samehada_util.StrideAdd(rangeStartKey, diffToMakeNoExist).(T)
					tmpEndKey := samehada_util.StrideAdd(rangeEndKey, diffToMakeNoExist).(T)
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, &tmpStartKey, &tmpEndKey, indexKind)
				}

				txn_ := txnMgr.Begin(nil)
				txn_.SetDebugInfo("Select(Range)-Op")
				results := executePlan(c, shi.GetBufferPoolManager(), txn_, rangeScanPlan)

				if txn_.GetState() == access.ABORTED {
					finalizeRandomNoSideEffectTxn(txn_)
					if execType == PARALLEL_EXEC {
						ch <- 1
					}
					return
				}

				if indexKind == index_constants.INDEX_KIND_SKIP_LIST {
					resultsLen := len(results)
					var prevVal *types.Value = nil
					for jj := 0; jj < resultsLen; jj++ {
						curVal := results[jj].GetValue(tableMetadata.Schema(), 0)

						if prevVal != nil {
							common.SH_Assert(curVal.CompareGreaterThanOrEqual(*prevVal), "values should be "+fmt.Sprintf("%v > %v", curVal.ToIFValue(), (*prevVal).ToIFValue()))
						}
						prevVal = &curVal
					}
				}
				finalizeRandomNoSideEffectTxn(txn_)
				if execType == PARALLEL_EXEC {
					ch <- 1
				}
			}
			if execType == PARALLEL_EXEC {
				go randomRangeScanOpFunc()
			} else {
				randomRangeScanOpFunc()
			}
		}
		runningThCnt++
	}

	// final checking of DB stored data
	// below, txns are execurted serial. so, txn abort due to CC protocol doesn't occur

	// check txn finished state and print these statistics
	common.SH_Assert(commitedTxnCnt+abortedTxnCnt == executedTxnCnt, "txn counting has bug(1)!")
	fmt.Printf("commited: %d aborted: %d all: %d (1)\n", commitedTxnCnt, abortedTxnCnt, executedTxnCnt)
	fmt.Printf("len(insVals):%d len(checkKeyColDupMap):%d\n", len(insVals), len(checkKeyColDupMap))

	// check total volume of accounts
	checkTotalBalanceNoChange()

	// check counts and order of all record got with index used full scan

	// col1 ---------------------------------
	txn_ := txnMgr.Begin(nil)
	txn_.MakeNotAbortable()

	// check record num (index of col1 is used)
	collectNumMaybe := insertedTupleCnt - deletedTupleCnt

	rangeScanPlan1 := createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, nil, nil, indexKind)
	results1 := executePlan(c, shi.GetBufferPoolManager(), txn_, rangeScanPlan1)
	resultsLen1 := len(results1)
	common.SH_Assert(txn_.GetState() != access.ABORTED, "last tuple count check is aborted!(1)")
	for idx, tuple_ := range results1 {
		common.SH_Assert(tuple_.Size() != 0, fmt.Sprintf("checked tuple's size is zero!!! idx=%d", idx))
	}

	// check value duplication (first column)
	rsltCheckMap := make(map[T]T, 0)
	for _, tuple_ := range results1 {
		val := tuple_.GetValue(tableMetadata.Schema(), 0).ToIFValue()
		castedVal := val.(T)
		if _, ok := rsltCheckMap[castedVal]; ok {
			fmt.Printf("duplicated key found on result1! rid:%v val:%v\n", tuple_.GetRID(), castedVal)
		} else {
			rsltCheckMap[castedVal] = castedVal
		}
	}

	// detect tuple which has illegal value at first column (unknown key based value)
	// -- make map having values which should be in DB
	okValMap := make(map[T]T, 0)
	for _, baseKey := range insVals {
		for ii := int32(0); ii < stride; ii++ {
			okVal := samehada_util.StrideAdd(samehada_util.StrideMul(baseKey, stride), ii).(T)
			okValMap[okVal] = okVal
		}
	}
	// -- check values on results1
	for _, tuple_ := range results1 {
		val := tuple_.GetValue(tableMetadata.Schema(), 0).ToIFValue()
		castedVal := val.(T)
		if _, ok := okValMap[castedVal]; !ok {
			if !samehada_util.IsContainList[T](accountIds, castedVal) {
				fmt.Printf("illegal key found on result1! rid:%v val:%v\n", tuple_.GetRID(), castedVal)
			}
		}
	}

	common.SH_Assert(collectNumMaybe == int32(resultsLen1), "records count is not matched with assumed num "+fmt.Sprintf("%d != %d", collectNumMaybe, resultsLen1))
	finalizeRandomNoSideEffectTxn(txn_)

	if indexKind == index_constants.INDEX_KIND_SKIP_LIST {
		// check order (col1 when index of it is used)
		txn_ = txnMgr.Begin(nil)
		txn_.MakeNotAbortable()
		var prevVal1 *types.Value = nil
		for jj := 0; jj < resultsLen1; jj++ {
			curVal1 := results1[jj].GetValue(tableMetadata.Schema(), 0)
			if prevVal1 != nil {
				common.SH_Assert(curVal1.CompareGreaterThanOrEqual(*prevVal1), "values should be "+fmt.Sprintf("%v > %v", curVal1.ToIFValue(), (*prevVal1).ToIFValue()))
			}
			prevVal1 = &curVal1
		}
		finalizeRandomNoSideEffectTxn(txn_)
	}
	// ---------------------------------------

	// col2 ----------------------------------
	txn_ = txnMgr.Begin(nil)
	txn_.MakeNotAbortable()

	//check record num (index of col2 is used)
	rangeScanPlan2 := createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 1, nil, nil, indexKind)
	results2 := executePlan(c, shi.GetBufferPoolManager(), txn_, rangeScanPlan2)
	resultsLen2 := len(results2)
	common.SH_Assert(txn_.GetState() != access.ABORTED, "last tuple count check is aborted!(2)")
	//common.SH_Assert(collectNumMaybe == int32(resultsLen2), "records count is not matched with assumed num "+fmt.Sprintf("%d != %d", collectNumMaybe, resultsLen2))
	fmt.Printf("collectNumMaybe:%d == resultsLen2:%d\n", collectNumMaybe, resultsLen2)
	finalizeRandomNoSideEffectTxn(txn_)

	if indexKind == index_constants.INDEX_KIND_SKIP_LIST {
		// check order (col2 when index of it is used)
		txn_ = txnMgr.Begin(nil)
		txn_.MakeNotAbortable()
		var prevVal2 *types.Value = nil
		for jj := 0; jj < resultsLen2; jj++ {
			curVal2 := results2[jj].GetValue(tableMetadata.Schema(), 1)
			if prevVal2 != nil {
				common.SH_Assert(curVal2.CompareGreaterThanOrEqual(*prevVal2), "values should be "+fmt.Sprintf("%v > %v", curVal2.ToIFValue(), (*prevVal2).ToIFValue()))
			}
			prevVal2 = &curVal2
		}
		finalizeRandomNoSideEffectTxn(txn_)
	}
	// --------------------------------------

	// detect tuple which has illegal value at second column (unknown key based value)
	// -- make map having values which should be in DB
	okValMap2 := make(map[int32]int32, 0)
	for _, baseKey := range insVals {
		for ii := int32(0); ii < stride; ii++ {
			okValBasedKey := samehada_util.StrideAdd(samehada_util.StrideMul(baseKey, stride), ii).(T)
			okVal := getInt32ValCorrespondToPassVal(okValBasedKey)
			okValMap2[okVal] = okVal
		}
	}
	// -- check values on results2
	for _, tuple_ := range results2 {
		val1 := tuple_.GetValue(tableMetadata.Schema(), 0).ToIFValue()
		val2 := tuple_.GetValue(tableMetadata.Schema(), 1).ToInteger()
		if _, ok := okValMap2[val2]; !ok {
			if _, ok = checkKeyColDupMap[val1.(T)]; !ok {
				fmt.Printf("illegal key found on result2! rid:%v val2:%v\n", tuple_.GetRID(), val2)
			}
		}
	}

	// TODO: for debugging
	fmt.Printf("NewRIDAtNormal:%v NewRIDAtRollback:%v\n", common.NewRIDAtNormal, common.NewRIDAtRollback)

	// tuple num check with full scan by seqScan ----------------------------------

	txn_ = txnMgr.Begin(nil)
	txn_.MakeNotAbortable()
	fullScanPlan := createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, -1, nil, nil, index_constants.INDEX_KIND_INVALID)
	results3 := executePlan(c, shi.GetBufferPoolManager(), txn_, fullScanPlan)
	resultsLen3 := len(results3)
	common.SH_Assert(txn_.GetState() != access.ABORTED, "last tuple count check is aborted!(3)")
	fmt.Printf("resultsLen3: %d\n", resultsLen3)
	//common.SH_Assert(collectNumMaybe == int32(resultsLen3), "records count is not matched with assumed num "+fmt.Sprintf("%d != %d (full scan by seqScan)", collectNumMaybe, resultsLen3))
	finalizeRandomNoSideEffectTxn(txn_)

	//----

	//common.SH_Assert(collectNumMaybe == int32(resultsLen2), "records count is not matched with assumed num "+fmt.Sprintf("%d != %d", collectNumMaybe, resultsLen2))

	common.SH_Assert(commitedTxnCnt+abortedTxnCnt == executedTxnCnt, "txn counting has bug(2)!")
	fmt.Printf("commited: %d aborted: %d all: %d (2)\n", commitedTxnCnt, abortedTxnCnt, executedTxnCnt)

	shi.CloseFilesForTesting()
}

func testSkipListParallelTxnStrideRoot[T int32 | float32 | string](t *testing.T, keyType types.TypeID) {
	bpoolSize := int32(500)
	//bpoolSize := int32(100)

	switch keyType {
	case types.Integer:
		//testParallelTxnsQueryingUniqSkipListIndexUsedColumns[T](t, keyType, 400, 3000, 13, 0, bpoolSize, index_constants.INDEX_KIND_UNIQ_SKIP_LIST, PARALLEL_EXEC, 20)
		//testParallelTxnsQueryingUniqSkipListIndexUsedColumns[T](t, keyType, 400, 30000, 13, 0, bpoolSize, index_constants.INDEX_KIND_UNIQ_SKIP_LIST, PARALLEL_EXEC, 20)
		//testParallelTxnsQueryingUniqSkipListIndexUsedColumns[T](t, keyType, 400, 30000, 13, 0, bpoolSize, index_constants.INDEX_KIND_UNIQ_SKIP_LIST, PARALLEL_EXEC, 20)
		testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 30000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC, 20)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 3000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC, 20)
	case types.Float:
		//testParallelTxnsQueryingUniqSkipListIndexUsedColumns[T](t, keyType, 400, 30000, 13, 0, bpoolSize, index_constants.INDEX_KIND_UNIQ_SKIP_LIST, PARALLEL_EXEC, 20)
		testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 240, 1000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC, 20)
	case types.Varchar:
		//testParallelTxnsQueryingUniqSkipListIndexUsedColumns[T](t, keyType, 400, 400, 13, 0, bpoolSize, index_constants.INDEX_KIND_INVALID, PARALLEL_EXEC, 20)
		//testParallelTxnsQueryingUniqSkipListIndexUsedColumns[T](t, keyType, 400, 3000, 13, 0, bpoolSize, index_constants.INDEX_KIND_UNIQ_SKIP_LIST, PARALLEL_EXEC, 20)

		//testParallelTxnsQueryingUniqSkipListIndexUsedColumns[T](t, keyType, 400, 90000, 17, 0, bpoolSize, index_constants.INDEX_KIND_UNIQ_SKIP_LIST, PARALLEL_EXEC, 20)
		testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 5000, 17, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC, 20)
		//testParallelTxnsQueryingUniqSkipListIndexUsedColumns[T](t, keyType, 400, 50000, 17, 0, bpoolSize, index_constants.INDEX_KIND_UNIQ_SKIP_LIST, PARALLEL_EXEC, 20)

		//testParallelTxnsQueryingUniqSkipListIndexUsedColumns[T](t, keyType, 400, 300, 17, 0, bpoolSize, index_constants.INDEX_KIND_UNIQ_SKIP_LIST, PARALLEL_EXEC, 20)
		//testParallelTxnsQueryingUniqSkipListIndexUsedColumns[T](t, keyType, 400, 500, 17, 0, bpoolSize, index_constants.INDEX_KIND_UNIQ_SKIP_LIST, SERIAL_EXEC, 20)
	default:
		panic("not implemented!")
	}
}

func TestKeyDuplicateInsertDeleteWithSkipListIndexInt(t *testing.T) {
	testKeyDuplicateInsertDeleteWithSkipListIndex[int32](t, types.Integer)
}

func TestKeyDuplicateInsertDeleteWithSkipListIndexFloat(t *testing.T) {
	testKeyDuplicateInsertDeleteWithSkipListIndex[float32](t, types.Float)
}

func TestKeyDuplicateInsertDeleteWithSkipListIndexVarchar(t *testing.T) {
	testKeyDuplicateInsertDeleteWithSkipListIndex[string](t, types.Varchar)
}

func TestSkipListPrallelTxnStrideInteger(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	testSkipListParallelTxnStrideRoot[int32](t, types.Integer)
}

func TestSkipListPrallelTxnStrideFloat(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	testSkipListParallelTxnStrideRoot[float32](t, types.Float)
}

func TestSkipListPrallelTxnStrideVarchar(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	testSkipListParallelTxnStrideRoot[string](t, types.Varchar)
}

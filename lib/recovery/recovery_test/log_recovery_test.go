package log_recovery

import (
	"bytes"
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/samehada"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	testingpkg "github.com/ryogrid/SamehadaDB/lib/testing/testing_assert"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/recovery/log_recovery"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/column"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

func TestRedo(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	samehadaInstance := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)

	samehadaInstance.GetLogManager().DeactivateLogging()
	testingpkg.AssertFalse(t, samehadaInstance.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("Skip system recovering...")

	samehadaInstance.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, samehadaInstance.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging thread running...")

	fmt.Println("Create a test table")
	txn := samehadaInstance.GetTransactionManager().Begin(nil)
	testTable := access.NewTableHeap(samehadaInstance.GetBufferPoolManager(), samehadaInstance.GetLogManager(),
		samehadaInstance.GetLockManager(), txn)

	var rid *page.RID
	var rid1 *page.RID
	col1 := column.NewColumn("a", types.Varchar, false, index_constants.IndexKindInvalid, types.PageID(-1), nil)
	col2 := column.NewColumn("b", types.Integer, false, index_constants.IndexKindInvalid, types.PageID(-1), nil)
	cols := []*column.Column{col1, col2}
	sc := schema.NewSchema(cols)
	tpl := ConstructTuple(sc)
	tpl1 := ConstructTuple(sc)

	valAt1 := tpl.GetValue(sc, 1)
	valAt0 := tpl.GetValue(sc, 0)
	val1At1 := tpl1.GetValue(sc, 1)
	val1At0 := tpl1.GetValue(sc, 0)

	rid, _ = testTable.InsertTuple(tpl, txn, math.MaxUint32, false)
	testingpkg.Assert(t, rid != nil, "")
	rid1, _ = testTable.InsertTuple(tpl1, txn, math.MaxUint32, false)
	testingpkg.Assert(t, rid != nil, "")

	samehadaInstance.GetTransactionManager().Commit(nil, txn)
	fmt.Println("Commit txn")

	fmt.Println("Shutdown System")
	samehadaInstance.CloseFilesForTesting()

	fmt.Println("System restart...")
	samehadaInstance = samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)

	samehadaInstance.GetLogManager().DeactivateLogging()
	testingpkg.AssertFalse(t, samehadaInstance.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("Check if tuple is not in table before recovery")
	txn = samehadaInstance.GetTransactionManager().Begin(nil)
	txn.SetIsRecoveryPhase(true)
	testTable = access.NewTableHeap(
		samehadaInstance.GetBufferPoolManager(),
		samehadaInstance.GetLogManager(),
		samehadaInstance.GetLockManager(),
		txn)
	oldTuple, _ := testTable.GetTuple(rid, txn)
	testingpkg.AssertFalse(t, oldTuple != nil, "")
	oldTuple1, _ := testTable.GetTuple(rid1, txn)
	testingpkg.AssertFalse(t, oldTuple1 != nil, "")
	samehadaInstance.GetTransactionManager().Commit(nil, txn)

	txn = samehadaInstance.GetTransactionManager().Begin(nil)
	txn.SetIsRecoveryPhase(true)
	fmt.Println("Begin recovery")
	lr := log_recovery.NewLogRecovery(
		samehadaInstance.GetDiskManager(),
		samehadaInstance.GetBufferPoolManager(),
		samehadaInstance.GetLogManager())

	testingpkg.AssertFalse(t, samehadaInstance.GetLogManager().IsEnabledLogging(), "")

	fmt.Println("Redo underway...")
	lr.Redo(txn)
	fmt.Println("Undo underway...")
	lr.Undo(txn)

	fmt.Println("Check if recovery success")
	txn = samehadaInstance.GetTransactionManager().Begin(nil)
	txn.SetIsRecoveryPhase(true)

	testTable = access.NewTableHeap(
		samehadaInstance.GetBufferPoolManager(),
		samehadaInstance.GetLogManager(),
		samehadaInstance.GetLockManager(),
		txn)

	oldTuple, _ = testTable.GetTuple(rid, txn)
	testingpkg.Assert(t, oldTuple != nil, "")
	oldTuple1, _ = testTable.GetTuple(rid1, txn)
	testingpkg.Assert(t, oldTuple1 != nil, "")

	samehadaInstance.GetTransactionManager().Commit(nil, txn)

	testingpkg.Assert(t, oldTuple.GetValue(sc, 1).CompareEquals(valAt1), "")
	testingpkg.Assert(t, oldTuple.GetValue(sc, 0).CompareEquals(valAt0), "")
	testingpkg.Assert(t, oldTuple1.GetValue(sc, 1).CompareEquals(val1At1), "")
	testingpkg.Assert(t, oldTuple1.GetValue(sc, 0).CompareEquals(val1At0), "")

	fmt.Println("Tearing down the system..")

	common.TempSuppressOnMemStorage = false
	samehadaInstance.Shutdown(samehada.ShutdownPatternRemoveFiles)
	common.TempSuppressOnMemStorageMutex.Unlock()
}

func TestUndo(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	samehadaInstance := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)

	samehadaInstance.GetLogManager().DeactivateLogging()
	testingpkg.AssertFalse(t, samehadaInstance.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("Skip system recovering...")

	samehadaInstance.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, samehadaInstance.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging thread running...")

	fmt.Println("Create a test table")
	txn := samehadaInstance.GetTransactionManager().Begin(nil)
	testTable := access.NewTableHeap(
		samehadaInstance.GetBufferPoolManager(),
		samehadaInstance.GetLogManager(),
		samehadaInstance.GetLockManager(),
		txn)
	firstPageID := testTable.GetFirstPageID()

	col1 := column.NewColumn("a", types.Varchar, false, index_constants.IndexKindInvalid, types.PageID(-1), nil)
	col2 := column.NewColumn("b", types.Integer, false, index_constants.IndexKindInvalid, types.PageID(-1), nil)
	cols := []*column.Column{col1, col2}

	sc := schema.NewSchema(cols)
	tuple1 := ConstructTuple(sc)
	val1At0 := tuple1.GetValue(sc, 0)
	val1At1 := tuple1.GetValue(sc, 1)
	var rid1 *page.RID
	fmt.Println("tuple1: ", tuple1.Data())
	rid1, _ = testTable.InsertTuple(tuple1, txn, math.MaxUint32, false)
	testingpkg.Assert(t, rid1 != nil, "")

	tuple2 := ConstructTuple(sc)
	val2At0 := tuple2.GetValue(sc, 0)
	val2At1 := tuple2.GetValue(sc, 1)

	var rid2 *page.RID
	rid2, _ = testTable.InsertTuple(tuple2, txn, math.MaxUint32, false)
	testingpkg.Assert(t, rid2 != nil, "")

	bfCommitTuple2, _ := testTable.GetTuple(rid2, txn)
	fmt.Println("bfCommitTuple2: ", bfCommitTuple2.Data())

	fmt.Println("Log page content is written to disk")
	samehadaInstance.GetLogManager().Flush()
	fmt.Println("two tuples inserted are commited")
	samehadaInstance.GetTransactionManager().Commit(nil, txn)

	txn = samehadaInstance.GetTransactionManager().Begin(nil)

	bfCommitTuple2b, _ := testTable.GetTuple(rid2, txn)
	fmt.Println("bf_commit_tuple2_: ", bfCommitTuple2b.Data())

	// tuple deletion (rid1)
	testTable.MarkDelete(rid1, math.MaxUint32, txn, false)

	bfCommitTuple2c, _ := testTable.GetTuple(rid2, txn)
	fmt.Println("bf_commit_tuple2_: ", bfCommitTuple2c.Data())

	// tuple updating (rid2)
	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewVarchar("updated"))
	row1 = append(row1, types.NewInteger(256))
	isSuccess, newRID, err, updateTuple, oldTuple := testTable.UpdateTuple(tuple.NewTupleFromSchema(row1, sc), nil, nil, math.MaxUint32, *rid2, txn, false)
	fmt.Println("returned values of testTable.Update_Tuple: ", isSuccess, newRID, err, updateTuple, updateTuple.Data(), oldTuple, oldTuple.Data())
	oldValue := oldTuple.GetValue(sc, 0)
	updateValue := updateTuple.GetValue(sc, 0)
	fmt.Println("oldValue: ", oldValue, "updateValue: ", updateValue)

	// tuple insertion (rid3)
	tuple3 := ConstructTuple(sc)
	var rid3 *page.RID
	rid3, _ = testTable.InsertTuple(tuple3, txn, math.MaxUint32, false)
	testingpkg.Assert(t, rid3 != nil, "")

	afInsertTuple2, _ := testTable.GetTuple(rid2, txn)
	fmt.Println("af_update_tuple2_: ", afInsertTuple2.Data())

	fmt.Println("Log page content is written to disk")
	samehadaInstance.GetLogManager().Flush()
	fmt.Println("Table page content is written to disk")
	samehadaInstance.GetBufferPoolManager().FlushPage(firstPageID)

	fmt.Println("System crash before commit")
	// delete samehadaInstance
	samehadaInstance.Shutdown(samehada.ShutdownPatternCloseFiles)

	fmt.Println("System restarted..")
	samehadaInstance = samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	txn = samehadaInstance.GetTransactionManager().Begin(nil)

	testTable = access.NewTableHeap(
		samehadaInstance.GetBufferPoolManager(),
		samehadaInstance.GetLogManager(),
		samehadaInstance.GetLockManager(),
		txn)

	fmt.Println("Check if deleted tuple does not exist before recovery")
	oldTuple1, _ := testTable.GetTuple(rid1, txn)
	testingpkg.Assert(t, oldTuple1 == nil, "handled as self deleted case")

	fmt.Println("Check if updated tuple values are effected before recovery")
	var oldTuple2 *tuple.Tuple

	if newRID == nil {
		oldTuple2, _ = testTable.GetTuple(rid2, txn)
		fmt.Println("oldTuple2: ", oldTuple2.Data())

		testingpkg.Assert(t, oldTuple2 != nil, "")
		testingpkg.Assert(t, oldTuple2.GetValue(sc, 0).CompareEquals(types.NewVarchar("updated")), "")
		testingpkg.Assert(t, oldTuple2.GetValue(sc, 1).CompareEquals(types.NewInteger(256)), "")
	} else {
		oldTuple2, _ = testTable.GetTuple(newRID, txn)
		fmt.Println("oldTuple2: ", oldTuple2.Data())

		testingpkg.Assert(t, oldTuple2 != nil, "")
		testingpkg.Assert(t, oldTuple2.GetValue(sc, 0).CompareEquals(types.NewVarchar("updated")), "")
		testingpkg.Assert(t, oldTuple2.GetValue(sc, 1).CompareEquals(types.NewInteger(256)), "")
	}

	fmt.Println("Check if inserted tuple exists before recovery")
	oldTuple3, _ := testTable.GetTuple(rid3, txn)
	testingpkg.Assert(t, oldTuple3 != nil, "")

	samehadaInstance.GetLogManager().DeactivateLogging()
	testingpkg.AssertFalse(t, samehadaInstance.GetLogManager().IsEnabledLogging(), "common.EnableLogging is not false!")

	fmt.Println("Recovery started..")
	lr := log_recovery.NewLogRecovery(
		samehadaInstance.GetDiskManager(),
		samehadaInstance.GetBufferPoolManager(),
		samehadaInstance.GetLogManager())

	samehadaInstance.GetLogManager().DeactivateLogging()
	testingpkg.AssertFalse(t, samehadaInstance.GetLogManager().IsEnabledLogging(), "")
	txn = samehadaInstance.GetTransactionManager().Begin(nil)
	txn.SetIsRecoveryPhase(true)

	lr.Redo(txn)
	fmt.Println("Redo underway...")
	lr.Undo(txn)
	fmt.Println("Undo underway...")

	fmt.Println("Check if failed txn is undo successfully")
	txn = samehadaInstance.GetTransactionManager().Begin(nil)

	fmt.Println("Check deleted tuple exists")
	oldTuple1, _ = testTable.GetTuple(rid1, txn)
	testingpkg.Assert(t, oldTuple1 != nil, "")
	testingpkg.Assert(t, oldTuple1.GetValue(sc, 0).CompareEquals(val1At0), "")
	testingpkg.Assert(t, oldTuple1.GetValue(sc, 1).CompareEquals(val1At1), "")

	fmt.Println("Check updated tuple's values are rollbacked")
	oldTuple2, _ = testTable.GetTuple(rid2, txn)
	testingpkg.Assert(t, oldTuple2 != nil, "")
	testingpkg.Assert(t, oldTuple2.GetValue(sc, 0).CompareEquals(val2At0), "")
	testingpkg.Assert(t, oldTuple2.GetValue(sc, 1).CompareEquals(val2At1), "")

	fmt.Println("Check inserted tuple does not exist")
	oldTuple3, _ = testTable.GetTuple(rid3, txn)
	testingpkg.Assert(t, oldTuple3 == nil, "")

	fmt.Println("Tearing down the system..")

	common.TempSuppressOnMemStorage = false
	samehadaInstance.Shutdown(samehada.ShutdownPatternRemoveFiles)
	common.TempSuppressOnMemStorageMutex.Unlock()
}

func TestCheckpoint(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}
	samehadaInstance := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)

	samehadaInstance.GetLogManager().DeactivateLogging()
	testingpkg.AssertFalse(t, samehadaInstance.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("Skip system recovering...")

	samehadaInstance.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, samehadaInstance.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging thread running...")

	fmt.Println("Create a test table")
	txn := samehadaInstance.GetTransactionManager().Begin(nil)
	testTable := access.NewTableHeap(
		samehadaInstance.GetBufferPoolManager(),
		samehadaInstance.GetLogManager(),
		samehadaInstance.GetLockManager(),
		txn)
	samehadaInstance.GetTransactionManager().Commit(nil, txn)

	col1 := column.NewColumn("a", types.Varchar, false, index_constants.IndexKindInvalid, types.PageID(-1), nil)
	col2 := column.NewColumn("b", types.Integer, false, index_constants.IndexKindInvalid, types.PageID(-1), nil)
	cols := []*column.Column{col1, col2}
	sc := schema.NewSchema(cols)

	tpl := ConstructTuple(sc)
	_ = tpl.GetValue(sc, 0)
	_ = tpl.GetValue(sc, 1)

	// set log time out very high so that flush doesn't happen before checkpoint is performed

	common.LogTimeout, _ = time.ParseDuration("15s") //seconds(15) //chrono::seconds(15)

	// insert a ton of tuples
	txn1 := samehadaInstance.GetTransactionManager().Begin(nil)
	for i := 0; i < 1000; i++ {
		rid, err := testTable.InsertTuple(tpl, txn1, math.MaxUint32, false)
		if err != nil {
			fmt.Println(err)
		}
		testingpkg.Assert(t, rid != nil, "")
	}
	samehadaInstance.GetTransactionManager().Commit(nil, txn1)

	// Do checkpoint
	samehadaInstance.GetCheckpointManager().BeginCheckpoint()
	samehadaInstance.GetCheckpointManager().EndCheckpoint()

	pages := samehadaInstance.GetBufferPoolManager().GetPages()
	poolSize := samehadaInstance.GetBufferPoolManager().GetPoolSize()

	// make sure that all pages in the buffer pool are marked as non-dirty
	allPagesClean := true
	for i := 0; i < poolSize; i++ {
		page := pages[i]
		pageID := page.GetPageID()

		if pageID != common.InvalidPageID && page.IsDirty() {
			allPagesClean = false
			break
		}
	}
	testingpkg.Assert(t, allPagesClean, "")

	// compare each page in the buffer pool to that page's
	// data on disk. ensure they match after the checkpoint
	allPagesMatch := true
	diskData := make([]byte, common.PageSize)
	for i := 0; i < poolSize; i++ {
		page := pages[i]
		pageID := page.GetPageID()

		if pageID != common.InvalidPageID {
			dmgrImpl := samehadaInstance.GetDiskManager()
			dmgrImpl.ReadPage(pageID, diskData)
			if !bytes.Equal(diskData[:common.PageSize], page.GetData()[:common.PageSize]) {
				allPagesMatch = false
				break
			}
		}
	}

	testingpkg.Assert(t, allPagesMatch, "")

	// Verify all committed transactions flushed to disk
	persistentLSN := samehadaInstance.GetLogManager().GetPersistentLSN()
	nextLSN := samehadaInstance.GetLogManager().GetNextLSN()
	testingpkg.Assert(t, persistentLSN == (nextLSN-1), "")

	// verify log was flushed and each page's LSN <= persistent lsn
	allPagesLte := true
	for i := 0; i < poolSize; i++ {
		page := pages[i]
		pageID := page.GetPageID()

		if pageID != common.InvalidPageID && page.GetLSN() > persistentLSN {
			allPagesLte = false
			break
		}
	}

	testingpkg.Assert(t, allPagesLte, "")

	fmt.Println("Shutdown System")
	fmt.Println("Tearing down the system..")

	common.TempSuppressOnMemStorage = false
	samehadaInstance.Shutdown(samehada.ShutdownPatternRemoveFiles)
	common.TempSuppressOnMemStorageMutex.Unlock()
}

// use a fixed schema to construct a random tuple
func ConstructTuple(sc *schema.Schema) *tuple.Tuple {
	var values []types.Value

	v := types.NewInteger(-1) //&types.Value{types.Invalid, nil, nil, nil}

	seed := time.Now().UnixNano()
	rand.Seed(seed)

	colCnt := int(sc.GetColumnCount())
	for i := 0; i < colCnt; i++ {
		// get type
		col := sc.GetColumn(uint32(i))
		colType := col.GetType()
		switch colType {
		case types.Boolean:
			randVal := rand.Intn(math.MaxUint32) % 2
			if randVal == 0 {
				v = types.NewBoolean(false)
			} else {
				v = types.NewBoolean(true)
			}
		case types.Integer:
			v = types.NewInteger(int32(rand.Intn(math.MaxUint32)) % 1000)
		case types.Varchar:
			alphabets :=
				"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
			strLen := int(1 + rand.Intn(math.MaxUint32)%10)
			s := ""
			for j := 0; j < strLen; j++ {
				idx := rand.Intn(52)
				s = s + alphabets[idx:idx+1]
			}
			v = types.NewVarchar(s)
		default:
		}
		values = append(values, v)
	}
	return tuple.NewTupleFromSchema(values, sc)
}

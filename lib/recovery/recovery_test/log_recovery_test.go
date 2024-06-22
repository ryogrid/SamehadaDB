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

	samehada_instance := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)

	samehada_instance.GetLogManager().DeactivateLogging()
	testingpkg.AssertFalse(t, samehada_instance.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("Skip system recovering...")

	samehada_instance.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, samehada_instance.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging thread running...")

	fmt.Println("Create a test table")
	txn := samehada_instance.GetTransactionManager().Begin(nil)
	test_table := access.NewTableHeap(samehada_instance.GetBufferPoolManager(), samehada_instance.GetLogManager(),
		samehada_instance.GetLockManager(), txn)

	var rid *page.RID
	var rid1 *page.RID
	col1 := column.NewColumn("a", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	col2 := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	cols := []*column.Column{col1, col2}
	schema_ := schema.NewSchema(cols)
	tuple_ := ConstructTuple(schema_)
	tuple1_ := ConstructTuple(schema_)

	val_1 := tuple_.GetValue(schema_, 1)
	val_0 := tuple_.GetValue(schema_, 0)
	val1_1 := tuple1_.GetValue(schema_, 1)
	val1_0 := tuple1_.GetValue(schema_, 0)

	rid, _ = test_table.InsertTuple(tuple_, txn, math.MaxUint32, false)
	// TODO: (SDB) insert index entry if needed
	testingpkg.Assert(t, rid != nil, "")
	rid1, _ = test_table.InsertTuple(tuple1_, txn, math.MaxUint32, false)
	// TODO: (SDB) insert index entry if needed
	testingpkg.Assert(t, rid != nil, "")

	samehada_instance.GetTransactionManager().Commit(nil, txn)
	fmt.Println("Commit txn")

	fmt.Println("Shutdown System")
	samehada_instance.CloseFilesForTesting()

	fmt.Println("System restart...")
	samehada_instance = samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)

	samehada_instance.GetLogManager().DeactivateLogging()
	testingpkg.AssertFalse(t, samehada_instance.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("Check if tuple is not in table before recovery")
	txn = samehada_instance.GetTransactionManager().Begin(nil)
	txn.SetIsRecoveryPhase(true)
	test_table = access.NewTableHeap(
		samehada_instance.GetBufferPoolManager(),
		samehada_instance.GetLogManager(),
		samehada_instance.GetLockManager(),
		txn)
	old_tuple, _ := test_table.GetTuple(rid, txn)
	testingpkg.AssertFalse(t, old_tuple != nil, "")
	old_tuple1, _ := test_table.GetTuple(rid1, txn)
	testingpkg.AssertFalse(t, old_tuple1 != nil, "")
	samehada_instance.GetTransactionManager().Commit(nil, txn)

	txn = samehada_instance.GetTransactionManager().Begin(nil)
	txn.SetIsRecoveryPhase(true)
	fmt.Println("Begin recovery")
	log_recovery := log_recovery.NewLogRecovery(
		samehada_instance.GetDiskManager(),
		samehada_instance.GetBufferPoolManager(),
		samehada_instance.GetLogManager())

	testingpkg.AssertFalse(t, samehada_instance.GetLogManager().IsEnabledLogging(), "")

	fmt.Println("Redo underway...")
	log_recovery.Redo(txn)
	fmt.Println("Undo underway...")
	log_recovery.Undo(txn)

	fmt.Println("Check if recovery success")
	txn = samehada_instance.GetTransactionManager().Begin(nil)
	txn.SetIsRecoveryPhase(true)

	test_table = access.NewTableHeap(
		samehada_instance.GetBufferPoolManager(),
		samehada_instance.GetLogManager(),
		samehada_instance.GetLockManager(),
		txn)

	old_tuple, _ = test_table.GetTuple(rid, txn)
	testingpkg.Assert(t, old_tuple != nil, "")
	old_tuple1, _ = test_table.GetTuple(rid1, txn)
	testingpkg.Assert(t, old_tuple1 != nil, "")

	samehada_instance.GetTransactionManager().Commit(nil, txn)

	testingpkg.Assert(t, old_tuple.GetValue(schema_, 1).CompareEquals(val_1), "")
	testingpkg.Assert(t, old_tuple.GetValue(schema_, 0).CompareEquals(val_0), "")
	testingpkg.Assert(t, old_tuple1.GetValue(schema_, 1).CompareEquals(val1_1), "")
	testingpkg.Assert(t, old_tuple1.GetValue(schema_, 0).CompareEquals(val1_0), "")

	fmt.Println("Tearing down the system..")

	common.TempSuppressOnMemStorage = false
	samehada_instance.Shutdown(samehada.ShutdownPatternRemoveFiles)
	common.TempSuppressOnMemStorageMutex.Unlock()
}

func TestUndo(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	samehada_instance := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)

	samehada_instance.GetLogManager().DeactivateLogging()
	testingpkg.AssertFalse(t, samehada_instance.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("Skip system recovering...")

	samehada_instance.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, samehada_instance.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging thread running...")

	fmt.Println("Create a test table")
	txn := samehada_instance.GetTransactionManager().Begin(nil)
	test_table := access.NewTableHeap(
		samehada_instance.GetBufferPoolManager(),
		samehada_instance.GetLogManager(),
		samehada_instance.GetLockManager(),
		txn)
	first_page_id := test_table.GetFirstPageId()

	col1 := column.NewColumn("a", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	col2 := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	cols := []*column.Column{col1, col2}

	schema_ := schema.NewSchema(cols)
	tuple1 := ConstructTuple(schema_)
	val1_0 := tuple1.GetValue(schema_, 0)
	val1_1 := tuple1.GetValue(schema_, 1)
	var rid1 *page.RID
	fmt.Println("tuple1: ", tuple1.Data())
	rid1, _ = test_table.InsertTuple(tuple1, txn, math.MaxUint32, false)
	testingpkg.Assert(t, rid1 != nil, "")

	tuple2 := ConstructTuple(schema_)
	val2_0 := tuple2.GetValue(schema_, 0)
	val2_1 := tuple2.GetValue(schema_, 1)

	var rid2 *page.RID
	rid2, _ = test_table.InsertTuple(tuple2, txn, math.MaxUint32, false)
	testingpkg.Assert(t, rid2 != nil, "")

	bf_commit_tuple2, _ := test_table.GetTuple(rid2, txn)
	fmt.Println("bf_commit_tuple2: ", bf_commit_tuple2.Data())

	fmt.Println("Log page content is written to disk")
	samehada_instance.GetLogManager().Flush()
	fmt.Println("two tuples inserted are commited")
	samehada_instance.GetTransactionManager().Commit(nil, txn)

	txn = samehada_instance.GetTransactionManager().Begin(nil)

	bf_commit_tuple2__, _ := test_table.GetTuple(rid2, txn)
	fmt.Println("bf_commit_tuple2_: ", bf_commit_tuple2__.Data())

	// tuple deletion (rid1)
	test_table.MarkDelete(rid1, math.MaxUint32, txn, false)

	bf_commit_tuple2___, _ := test_table.GetTuple(rid2, txn)
	fmt.Println("bf_commit_tuple2_: ", bf_commit_tuple2___.Data())

	// tuple updating (rid2)
	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewVarchar("updated"))
	row1 = append(row1, types.NewInteger(256))
	is_success, new_rid, err, update_tuple, old_tuple := test_table.UpdateTuple(tuple.NewTupleFromSchema(row1, schema_), nil, nil, math.MaxUint32, *rid2, txn, false)
	fmt.Println("returned values of test_table.Update_Tuple: ", is_success, new_rid, err, update_tuple, update_tuple.Data(), old_tuple, old_tuple.Data())
	old_value := old_tuple.GetValue(schema_, 0)
	update_value := update_tuple.GetValue(schema_, 0)
	fmt.Println("old_value: ", old_value, "update_value: ", update_value)

	// tuple insertion (rid3)
	tuple3 := ConstructTuple(schema_)
	var rid3 *page.RID
	rid3, _ = test_table.InsertTuple(tuple3, txn, math.MaxUint32, false)
	// TODO: (SDB) insert index entry if needed
	testingpkg.Assert(t, rid3 != nil, "")

	af_insert_tuple2, _ := test_table.GetTuple(rid2, txn)
	fmt.Println("af_update_tuple2_: ", af_insert_tuple2.Data())

	fmt.Println("Log page content is written to disk")
	samehada_instance.GetLogManager().Flush()
	fmt.Println("Table page content is written to disk")
	samehada_instance.GetBufferPoolManager().FlushPage(first_page_id)

	fmt.Println("System crash before commit")
	// delete samehada_instance
	samehada_instance.Shutdown(samehada.ShutdownPatternCloseFiles)

	fmt.Println("System restarted..")
	samehada_instance = samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	txn = samehada_instance.GetTransactionManager().Begin(nil)

	test_table = access.NewTableHeap(
		samehada_instance.GetBufferPoolManager(),
		samehada_instance.GetLogManager(),
		samehada_instance.GetLockManager(),
		txn)

	fmt.Println("Check if deleted tuple does not exist before recovery")
	old_tuple1, _ := test_table.GetTuple(rid1, txn)
	testingpkg.Assert(t, old_tuple1 == nil, "handled as self deleted case")

	fmt.Println("Check if updated tuple values are effected before recovery")
	var old_tuple2 *tuple.Tuple

	if new_rid == nil {
		old_tuple2, _ = test_table.GetTuple(rid2, txn)
		fmt.Println("old_tuple2: ", old_tuple2.Data())

		testingpkg.Assert(t, old_tuple2 != nil, "")
		testingpkg.Assert(t, old_tuple2.GetValue(schema_, 0).CompareEquals(types.NewVarchar("updated")), "")
		testingpkg.Assert(t, old_tuple2.GetValue(schema_, 1).CompareEquals(types.NewInteger(256)), "")
	} else {
		old_tuple2, _ = test_table.GetTuple(new_rid, txn)
		fmt.Println("old_tuple2: ", old_tuple2.Data())

		testingpkg.Assert(t, old_tuple2 != nil, "")
		testingpkg.Assert(t, old_tuple2.GetValue(schema_, 0).CompareEquals(types.NewVarchar("updated")), "")
		testingpkg.Assert(t, old_tuple2.GetValue(schema_, 1).CompareEquals(types.NewInteger(256)), "")
	}

	fmt.Println("Check if inserted tuple exists before recovery")
	old_tuple3, _ := test_table.GetTuple(rid3, txn)
	testingpkg.Assert(t, old_tuple3 != nil, "")

	samehada_instance.GetLogManager().DeactivateLogging()
	testingpkg.AssertFalse(t, samehada_instance.GetLogManager().IsEnabledLogging(), "common.EnableLogging is not false!")

	fmt.Println("Recovery started..")
	log_recovery := log_recovery.NewLogRecovery(
		samehada_instance.GetDiskManager(),
		samehada_instance.GetBufferPoolManager(),
		samehada_instance.GetLogManager())

	samehada_instance.GetLogManager().DeactivateLogging()
	testingpkg.AssertFalse(t, samehada_instance.GetLogManager().IsEnabledLogging(), "")
	txn = samehada_instance.GetTransactionManager().Begin(nil)
	txn.SetIsRecoveryPhase(true)

	log_recovery.Redo(txn)
	fmt.Println("Redo underway...")
	log_recovery.Undo(txn)
	fmt.Println("Undo underway...")

	fmt.Println("Check if failed txn is undo successfully")
	txn = samehada_instance.GetTransactionManager().Begin(nil)

	fmt.Println("Check deleted tuple exists")
	old_tuple1, _ = test_table.GetTuple(rid1, txn)
	testingpkg.Assert(t, old_tuple1 != nil, "")
	testingpkg.Assert(t, old_tuple1.GetValue(schema_, 0).CompareEquals(val1_0), "")
	testingpkg.Assert(t, old_tuple1.GetValue(schema_, 1).CompareEquals(val1_1), "")

	fmt.Println("Check updated tuple's values are rollbacked")
	old_tuple2, _ = test_table.GetTuple(rid2, txn)
	testingpkg.Assert(t, old_tuple2 != nil, "")
	testingpkg.Assert(t, old_tuple2.GetValue(schema_, 0).CompareEquals(val2_0), "")
	testingpkg.Assert(t, old_tuple2.GetValue(schema_, 1).CompareEquals(val2_1), "")

	fmt.Println("Check inserted tuple does not exist")
	old_tuple3, _ = test_table.GetTuple(rid3, txn)
	testingpkg.Assert(t, old_tuple3 == nil, "")

	fmt.Println("Tearing down the system..")

	common.TempSuppressOnMemStorage = false
	samehada_instance.Shutdown(samehada.ShutdownPatternRemoveFiles)
	common.TempSuppressOnMemStorageMutex.Unlock()
}

func TestCheckpoint(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}
	samehada_instance := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)

	samehada_instance.GetLogManager().DeactivateLogging()
	testingpkg.AssertFalse(t, samehada_instance.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("Skip system recovering...")

	samehada_instance.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, samehada_instance.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging thread running...")

	fmt.Println("Create a test table")
	txn := samehada_instance.GetTransactionManager().Begin(nil)
	test_table := access.NewTableHeap(
		samehada_instance.GetBufferPoolManager(),
		samehada_instance.GetLogManager(),
		samehada_instance.GetLockManager(),
		txn)
	samehada_instance.GetTransactionManager().Commit(nil, txn)

	col1 := column.NewColumn("a", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	col2 := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	cols := []*column.Column{col1, col2}
	schema_ := schema.NewSchema(cols)

	tuple_ := ConstructTuple(schema_)
	_ = tuple_.GetValue(schema_, 0)
	_ = tuple_.GetValue(schema_, 1)

	// set log time out very high so that flush doesn't happen before checkpoint is performed

	common.LogTimeout, _ = time.ParseDuration("15s") //seconds(15) //chrono::seconds(15)

	// insert a ton of tuples
	txn1 := samehada_instance.GetTransactionManager().Begin(nil)
	for i := 0; i < 1000; i++ {
		rid, err := test_table.InsertTuple(tuple_, txn1, math.MaxUint32, false)
		if err != nil {
			fmt.Println(err)
		}
		// TODO: (SDB) insert index entry if needed
		testingpkg.Assert(t, rid != nil, "")
	}
	samehada_instance.GetTransactionManager().Commit(nil, txn1)

	// Do checkpoint
	samehada_instance.GetCheckpointManager().BeginCheckpoint()
	samehada_instance.GetCheckpointManager().EndCheckpoint()

	pages := samehada_instance.GetBufferPoolManager().GetPages()
	pool_size := samehada_instance.GetBufferPoolManager().GetPoolSize()

	// make sure that all pages in the buffer pool are marked as non-dirty
	all_pages_clean := true
	for i := 0; i < pool_size; i++ {
		page := pages[i]
		page_id := page.GetPageId()

		if page_id != common.InvalidPageID && page.IsDirty() {
			all_pages_clean = false
			break
		}
	}
	testingpkg.Assert(t, all_pages_clean, "")

	// compare each page in the buffer pool to that page's
	// data on disk. ensure they match after the checkpoint
	all_pages_match := true
	disk_data := make([]byte, common.PageSize)
	for i := 0; i < pool_size; i++ {
		page := pages[i]
		page_id := page.GetPageId()

		if page_id != common.InvalidPageID {
			dmgr_impl := samehada_instance.GetDiskManager()
			dmgr_impl.ReadPage(page_id, disk_data)
			if !bytes.Equal(disk_data[:common.PageSize], page.GetData()[:common.PageSize]) {
				all_pages_match = false
				break
			}
		}
	}

	testingpkg.Assert(t, all_pages_match, "")

	// Verify all committed transactions flushed to disk
	persistent_lsn := samehada_instance.GetLogManager().GetPersistentLSN()
	next_lsn := samehada_instance.GetLogManager().GetNextLSN()
	testingpkg.Assert(t, persistent_lsn == (next_lsn-1), "")

	// verify log was flushed and each page's LSN <= persistent lsn
	all_pages_lte := true
	for i := 0; i < pool_size; i++ {
		page := pages[i]
		page_id := page.GetPageId()

		if page_id != common.InvalidPageID && page.GetLSN() > persistent_lsn {
			all_pages_lte = false
			break
		}
	}

	testingpkg.Assert(t, all_pages_lte, "")

	fmt.Println("Shutdown System")
	fmt.Println("Tearing down the system..")

	common.TempSuppressOnMemStorage = false
	samehada_instance.Shutdown(samehada.ShutdownPatternRemoveFiles)
	common.TempSuppressOnMemStorageMutex.Unlock()
}

// use a fixed schema to construct a random tuple
func ConstructTuple(schema_ *schema.Schema) *tuple.Tuple {
	var values []types.Value

	v := types.NewInteger(-1) //&types.Value{types.Invalid, nil, nil, nil}

	seed := time.Now().UnixNano()
	rand.Seed(seed)

	col_cnt := int(schema_.GetColumnCount())
	for i := 0; i < col_cnt; i++ {
		// get type
		col := schema_.GetColumn(uint32(i))
		type_ := col.GetType()
		switch type_ {
		case types.Boolean:
			rand_val := rand.Intn(math.MaxUint32) % 2
			if rand_val == 0 {
				v = types.NewBoolean(false)
			} else {
				v = types.NewBoolean(true)
			}
		case types.Integer:
			v = types.NewInteger(int32(rand.Intn(math.MaxUint32)) % 1000)
		case types.Varchar:
			alphabets :=
				"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
			len_ := int(1 + rand.Intn(math.MaxUint32)%10)
			s := ""
			for j := 0; j < len_; j++ {
				idx := rand.Intn(52)
				s = s + alphabets[idx:idx+1]
			}
			v = types.NewVarchar(s)
		default:
		}
		values = append(values, v)
	}
	return tuple.NewTupleFromSchema(values, schema_)
}

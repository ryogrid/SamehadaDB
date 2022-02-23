package log_recovery

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/recovery/log_recovery"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/test_util"
	"github.com/ryogrid/SamehadaDB/types"
)

func TestRedo(t *testing.T) {
	// remove("test.db")
	// remove("test.log")
	os.Remove("test.db")
	os.Remove("test.log")

	samehada_instance := test_util.NewSamehadaInstance()

	testingpkg.AssertFalse(t, common.EnableLogging, "")
	fmt.Println("Skip system recovering...")

	samehada_instance.GetLogManager().RunFlushThread()
	testingpkg.Assert(t, common.EnableLogging, "")
	fmt.Println("System logging thread running...")

	fmt.Println("Create a test table")
	txn := samehada_instance.GetTransactionManager().Begin(nil)
	test_table := access.NewTableHeap(samehada_instance.GetBufferPoolManager(), samehada_instance.GetLogManager(),
		samehada_instance.GetLockManager(), txn)
	//first_page_id := test_table.GetFirstPageId()

	var rid *page.RID
	var rid1 *page.RID
	// col1 := &column.Column{"a", types.Varchar, 20}
	// col2 := &column.Column{"b", types.Smallint}
	col1 := column.NewColumn("a", types.Varchar)
	//col2 := column.NewColumn("b", types.Smallint)
	col2 := column.NewColumn("b", types.Integer)
	cols := []*column.Column{col1, col2}
	schema_ := schema.NewSchema(cols)
	tuple_ := ConstructTuple(schema_)
	tuple1_ := ConstructTuple(schema_)

	val_1 := tuple_.GetValue(schema_, 1)
	val_0 := tuple_.GetValue(schema_, 0)
	val1_1 := tuple1_.GetValue(schema_, 1)
	val1_0 := tuple1_.GetValue(schema_, 0)

	// TODO: (SDB) need check that assertion logic is collect
	// testingpkg.Assert(t, test_table.InsertTuple(tuple_, &rid, txn), "")
	// testingpkg.Assert(t, test_table.InsertTuple(tuple_1, &rid1, txn), "")
	rid, _ = test_table.InsertTuple(tuple_, txn)
	testingpkg.Assert(t, rid != nil, "")
	rid1, _ = test_table.InsertTuple(tuple_, txn)
	testingpkg.Assert(t, rid != nil, "")

	samehada_instance.GetTransactionManager().Commit(txn)
	fmt.Println("Commit txn")

	// delete txn
	// delete test_table

	fmt.Println("Shutdown System")
	// delete samehada_instance
	samehada_instance.Finalize(false)

	fmt.Println("System restart...")
	samehada_instance = test_util.NewSamehadaInstance()

	samehada_instance.GetLogManager().StopFlushThread()
	testingpkg.AssertFalse(t, common.EnableLogging, "")
	fmt.Println("Check if tuple is not in table before recovery")
	// var old_tuple tuple.Tuple
	// var old_tuple1 tuple.Tuple
	txn = samehada_instance.GetTransactionManager().Begin(nil)
	// test_table = access.NewTableHeap(samehada_instance.GetBufferPoolManager(), samehada_instance.GetLogManager(),
	// 	samehada_instance.GetLockManager(), first_page_id)
	test_table = access.NewTableHeap(
		samehada_instance.GetBufferPoolManager(),
		samehada_instance.GetLogManager(),
		samehada_instance.GetLockManager(),
		txn)
	old_tuple := test_table.GetTuple(rid, txn)
	testingpkg.AssertFalse(t, old_tuple != nil, "")
	old_tuple1 := test_table.GetTuple(rid1, txn)
	testingpkg.AssertFalse(t, old_tuple1 != nil, "")
	samehada_instance.GetTransactionManager().Commit(txn)
	// delete txn

	fmt.Println("Begin recovery")
	log_recovery := log_recovery.NewLogRecovery(
		samehada_instance.GetDiskManager(),
		samehada_instance.GetBufferPoolManager())

	testingpkg.AssertFalse(t, common.EnableLogging, "")

	fmt.Println("Redo underway...")
	log_recovery.Redo()
	fmt.Println("Undo underway...")
	log_recovery.Undo()

	fmt.Println("Check if recovery success")
	txn = samehada_instance.GetTransactionManager().Begin(nil)
	// delete test_table
	// test_table = access.NewTableHeap(samehada_instance.GetBufferPoolManager(), samehada_instance.GetLogManager(),
	// 	samehada_instance.GetLockManager(), first_page_id)
	test_table = access.NewTableHeap(
		samehada_instance.GetBufferPoolManager(),
		samehada_instance.GetLogManager(),
		samehada_instance.GetLockManager(),
		txn)

	// testingpkg.Assert(t, test_table.GetTuple(rid, &old_tuple, txn), "")
	// testingpkg.Assert(t, test_table.GetTuple(rid1, &old_tuple1, txn), "")
	old_tuple = test_table.GetTuple(rid, txn)
	testingpkg.Assert(t, old_tuple != nil, "")
	old_tuple1 = test_table.GetTuple(rid1, txn)
	testingpkg.Assert(t, old_tuple1 != nil, "")

	samehada_instance.GetTransactionManager().Commit(txn)
	// delete txn
	// delete test_table
	// delete log_recovery

	// testingpkg.Equals(t, old_tuple.GetValue(&schema_, 1).CompareEquals(val_1), CmpBool::CmpTrue)
	// testingpkg.Equals(t, old_tuple.GetValue(&schema_, 0).CompareEquals(val_0), CmpBool::CmpTrue)
	// testingpkg.Equals(t, old_tuple1.GetValue(&schema_, 1).CompareEquals(val1_1), CmpBool::CmpTrue)
	// testingpkg.Equals(t, old_tuple1.GetValue(&schema_, 0).CompareEquals(val1_0), CmpBool::CmpTrue)
	testingpkg.Assert(t, old_tuple.GetValue(schema_, 1).CompareEquals(val_1), "")
	testingpkg.Assert(t, old_tuple.GetValue(schema_, 0).CompareEquals(val_0), "")
	testingpkg.Assert(t, old_tuple1.GetValue(schema_, 1).CompareEquals(val1_1), "")
	testingpkg.Assert(t, old_tuple1.GetValue(schema_, 0).CompareEquals(val1_0), "")

	// delete samehada_instance
	fmt.Println("Tearing down the system..")
	samehada_instance.Finalize(true)
	// remove("test.db")
	// remove("test.log")
}

func TestUndo(t *testing.T) {
	// remove("test.db")
	// remove("test.log")
	os.Remove("test.db")
	os.Remove("test.log")

	samehada_instance := test_util.NewSamehadaInstance()

	testingpkg.AssertFalse(t, common.EnableLogging, "")
	fmt.Println("Skip system recovering...")

	samehada_instance.GetLogManager().RunFlushThread()
	testingpkg.Assert(t, common.EnableLogging, "")
	fmt.Println("System logging thread running...")

	fmt.Println("Create a test table")
	txn := samehada_instance.GetTransactionManager().Begin(nil)
	test_table := access.NewTableHeap(
		samehada_instance.GetBufferPoolManager(),
		samehada_instance.GetLogManager(),
		samehada_instance.GetLockManager(),
		txn)
	first_page_id := test_table.GetFirstPageId()

	// col1 := &column.Column{"a", types.Varchar, 20}
	// col2 := &column.Column{"b", types.Smallint}
	col1 := column.NewColumn("a", types.Varchar)
	//	col2 := column.NewColumn("b", types.Smallint)
	col2 := column.NewColumn("b", types.Integer)
	cols := []*column.Column{col1, col2}
	schema_ := schema.NewSchema(cols)
	tuple_ := ConstructTuple(schema_)

	val_0 := tuple_.GetValue(schema_, 0)
	val_1 := tuple_.GetValue(schema_, 1)

	// TODO: (SDB) need check that assertion logic is collect
	//testingpkg.Assert(t, test_table.InsertTuple(tuple_, &rid, txn), "")
	var rid *page.RID
	rid, _ = test_table.InsertTuple(tuple_, txn)
	testingpkg.Assert(t, rid != nil, "")

	fmt.Println("Table page content is written to disk")
	samehada_instance.GetBufferPoolManager().FlushPage(first_page_id)

	// delete txn
	// delete test_table

	fmt.Println("System crash before commit")
	// delete samehada_instance
	samehada_instance.Finalize(false)

	fmt.Println("System restarted..")
	samehada_instance = test_util.NewSamehadaInstance()

	fmt.Println("Check if tuple exists before recovery")
	var old_tuple *tuple.Tuple
	txn = samehada_instance.GetTransactionManager().Begin(nil)
	// test_table = access.NewTableHeap(samehada_instance.GetBufferPoolManager(), samehada_instance.GetLogManager(),
	// 	samehada_instance.GetLockManager(), first_page_id)
	test_table = access.NewTableHeap(
		samehada_instance.GetBufferPoolManager(),
		samehada_instance.GetLogManager(),
		samehada_instance.GetLockManager(),
		txn)

	//testingpkg.Assert(t, test_table.GetTuple(rid, &old_tuple, txn), "")
	old_tuple = test_table.GetTuple(rid, txn)
	testingpkg.Assert(t, old_tuple != nil, "")
	//testingpkg.Equals(t, old_tuple.GetValue(&schema_, 0).CompareEquals(val_0), CmpBool::CmpTrue)
	testingpkg.Assert(t, old_tuple.GetValue(schema_, 0).CompareEquals(val_0), "")
	//testingpkg.Assert(t, old_tuple.GetValue(&schema_, 1).CompareEquals(val_1), CmpBool::CmpTrue)
	testingpkg.Assert(t, old_tuple.GetValue(schema_, 1).CompareEquals(val_1), "")
	samehada_instance.GetTransactionManager().Commit(txn)
	// delete txn

	fmt.Println("Recovery started..")
	log_recovery := log_recovery.NewLogRecovery(
		samehada_instance.GetDiskManager(),
		samehada_instance.GetBufferPoolManager())

	samehada_instance.GetLogManager().StopFlushThread()
	testingpkg.AssertFalse(t, common.EnableLogging, "")

	log_recovery.Redo()
	fmt.Println("Redo underway...")
	log_recovery.Undo()
	fmt.Println("Undo underway...")

	fmt.Println("Check if failed txn is undo successfully")
	txn = samehada_instance.GetTransactionManager().Begin(nil)
	// delete test_table
	// test_table = access.NewTableHeap(samehada_instance.GetBufferPoolManager(), samehada_instance.GetLogManager(),
	// 	samehada_instance.GetLockManager(), first_page_id)
	test_table = access.NewTableHeap(
		samehada_instance.GetBufferPoolManager(),
		samehada_instance.GetLogManager(),
		samehada_instance.GetLockManager(),
		txn)

	//testingpkg.AssertFalse(t, test_table.GetTuple(rid, &old_tuple, txn), "")
	old_tuple = test_table.GetTuple(rid, txn)
	testingpkg.AssertFalse(t, old_tuple != nil, "")
	samehada_instance.GetTransactionManager().Commit(txn)

	// delete txn
	// delete test_table
	// delete log_recovery

	// delete samehada_instance
	fmt.Println("Tearing down the system..")
	samehada_instance.Finalize(true)
	// remove("test.db")
	// remove("test.log")
}

func EXPECT_TRUE(condition bool)                                        {}
func EXPECT_FALSE(condition bool)                                       {}
func EXPECT_EQ(arg1 interface{}, arg2 interface{})                      {}
func memcmp(arg1 interface{}, arg2 interface{}, arg3 interface{}) int32 { return -1 }

// TODO: (SDB) need prepare "expect" type testing utility func
// TODO: (SDB) need implement TestCheckPoint
/*
func TestCheckpoint(t *testing.T) {
	// remove("test.db")
	// remove("test.log")
	samehada_instance := test_util.NewSamehadaInstance()

	EXPECT_FALSE(common.EnableLogging)
	fmt.Println("Skip system recovering...")

	samehada_instance.GetLogManager().RunFlushThread()
	EXPECT_TRUE(common.EnableLogging)
	fmt.Println("System logging thread running...")

	fmt.Println("Create a test table")
	txn := samehada_instance.GetTransactionManager().Begin(nil)
	test_table := access.NewTableHeap(samehada_instance.GetBufferPoolManager(), samehada_instance.GetLogManager(),
		samehada_instance.GetLockManager(), txn)
	samehada_instance.GetTransactionManager().Commit(txn)

	// col1 := &column.Column{"a", types.Varchar, 20}
	// col2 := &column.Column{"b", types.Smallint}
	col1 := column.NewColumn("a", types.Varchar)
	col2 := column.NewColumn("b", types.Smallint)
	cols := []*column.Column{col1, col2}
	schema_ := schema.NewSchema(cols)

	tuple_ := ConstructTuple(schema_)
	// val_0 := tuple_.GetValue(schema_, 0)
	// val_1 := tuple_.GetValue(schema_, 1)
	_ = tuple_.GetValue(schema_, 0)
	_ = tuple_.GetValue(schema_, 1)

	// set log time out very high so that flush doesn't happen before checkpoint is performed

	common.LogTimeout, _ = time.ParseDuration("5s") //seconds(15) //chrono::seconds(15)

	// insert a ton of tuples
	txn1 := samehada_instance.GetTransactionManager().Begin(nil)
	for i := 0; i < 1000; i++ {
		var rid *page.RID = nil
		rid, _ = test_table.InsertTuple(tuple_, txn1)
		EXPECT_TRUE(rid != nil)
	}
	samehada_instance.GetTransactionManager().Commit(txn1)

	// Do checkpoint
	samehada_instance.GetCheckpointManager().BeginCheckpoint()
	samehada_instance.GetCheckpointManager().EndCheckpoint()

	// TODO: (SDB) need to implment BufferPoolManager::{GetPages, GetPoolSize}
	//             for checkpoint
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
	EXPECT_TRUE(all_pages_clean)

	// compare each page in the buffer pool to that page's
	// data on disk. ensure they match after the checkpoint
	all_pages_match := true
	disk_data := make([]byte, common.PageSize)
	for i := 0; i < pool_size; i++ {
		page := pages[i]
		page_id := page.GetPageId()

		if page_id != common.InvalidPageID {
			dmgr_impl := ((*disk.DiskManagerImpl)(unsafe.Pointer(samehada_instance.GetDiskManager())))
			//samehada_instance.GetDiskManager().ReadPage(page_id, disk_data)
			dmgr_impl.ReadPage(page_id, disk_data)
			if memcmp(disk_data, page.GetData(), common.PageSize) != 0 {
				all_pages_match = false
				break
			}
		}
	}

	EXPECT_TRUE(all_pages_match)
	// delete[] disk_data

	// Verify all committed transactions flushed to disk
	persistent_lsn := samehada_instance.GetLogManager().GetPersistentLSN()
	next_lsn := samehada_instance.GetLogManager().GetNextLSN()
	EXPECT_EQ(persistent_lsn, (next_lsn - 1))

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

	EXPECT_TRUE(all_pages_lte)

	// delete txn
	// delete txn1
	// delete test_table

	fmt.Println("Shutdown System")
	// delete samehada_instance

	fmt.Println("Tearing down the system..")
	// remove("test.db")
	// remove("test.log")
}
*/

// use a fixed schema to construct a random tuple
func ConstructTuple(schema_ *schema.Schema) *tuple.Tuple {
	var values []types.Value

	v := types.NewInteger(-1) //&types.Value{types.Invalid, nil, nil, nil}

	//seed = std::chrono::system_clock::now().time_since_epoch().count()
	seed := time.Now().UnixNano()
	rand.Seed(seed)

	col_cnt := int(schema_.GetColumnCount())
	//std::mt19937 generator(seed)  // mt19937 is a standard mersenne_twister_engine
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
		// case types.Tinyint:
		// 	v = Value(type_, int8(rand.Intn(math.MaxUint32)) % 1000)
		// 	break
		// case types.Smallint:
		// 	v = Value(type_, int16(rand.Intn(math.MaxUint32)) % 1000)
		// 	break
		case types.Integer:
			v = types.NewInteger(int32(rand.Intn(math.MaxUint32)) % 1000)
		// case types.BigInt:
		// 	v = Value(type_, int64(rand.Intn(math.MaxUint32)) % 1000)
		// 	break
		case types.Varchar:
			alphabets :=
				"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
			len := int(1 + rand.Intn(math.MaxUint32)%10)
			//char s[10]
			s := ""
			for j := 0; j < len; j++ {
				//s[j] = alphanum[rand.Intn(math.MaxUint32) % (sizeof(alphanum) - 1)]
				idx := rand.Intn(53)
				s = s + alphabets[idx:idx]
			}
			//s[len] = 0
			//v = Value(type, s, len + 1, true)
			v = types.NewVarchar(s)
		default:
		}
		values = append(values, v)
	}
	return tuple.NewTupleFromSchema(values, schema_)
	//return tuple.NewTuple(new(page.RID), 0, []byte{})
}

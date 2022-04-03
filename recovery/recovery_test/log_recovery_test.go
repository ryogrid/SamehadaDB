package log_recovery

import (
	"bytes"
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
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
)

/*
func TestLogSererializeAndDeserialize(t *testing.T) {
	os.Remove("test.log")

	// on this test, EnableLogging should no be true
	common.EnableLogging = false

	dm := disk.NewDiskManagerImpl("test.log")
	lm := recovery.NewLogManager(&dm)
	lr := log_recovery.NewLogRecovery(&dm, nil)
	tm := access.NewTransactionManager(lm)

	dummyTupleData1 := make([]byte, 100)
	for ii := 0; ii < 100; ii++ {
		// each Byte value is 7!
		dummyTupleData1[ii] = byte(7)
	}

	dummyTupleData2 := make([]byte, 100)
	for ii := 0; ii < 100; ii++ {
		// each Byte value is 11!
		dummyTupleData2[ii] = byte(11)
	}

	var cntup_num uint32 = 0
	//INSERT, APPLYDELETE, UPDATE, NEWPAGE
	for ii := 0; ii < 110; ii++ {
		txn := tm.Begin(nil)
		rid := new(page.RID)
		rid.Set(types.PageID(cntup_num), cntup_num)
		dummyData := make([]byte, 100)
		copy(dummyData, dummyTupleData1)
		tuple_ := tuple.NewTuple(rid, 100, dummyData)
		log_rec := recovery.NewLogRecordInsertDelete(txn.GetTransactionId(), txn.GetPrevLSN(),
			recovery.INSERT, *rid, tuple_)
		lsn := lm.AppendLogRecord(log_rec)
		txn.SetPrevLSN(lsn)
		cntup_num++
		////////////////////////

		txn = tm.Begin(nil)
		rid = new(page.RID)
		rid.Set(types.PageID(cntup_num), cntup_num)
		dummyData = make([]byte, 100)
		copy(dummyData, dummyTupleData1)
		tuple_old := tuple.NewTuple(rid, 100, dummyData)
		dummyData2 := make([]byte, 100)
		copy(dummyData2, dummyTupleData2)
		tuple_new := tuple.NewTuple(rid, 100, dummyData2)
		log_rec = recovery.NewLogRecordUpdate(txn.GetTransactionId(), txn.GetPrevLSN(),
			recovery.UPDATE, *rid, *tuple_old, *tuple_new)
		lsn = lm.AppendLogRecord(log_rec)
		txn.SetPrevLSN(lsn)
		cntup_num++
		////////////////////////

		txn = tm.Begin(nil)
		rid = new(page.RID)
		rid.Set(types.PageID(cntup_num), cntup_num)
		cntup_num++
		// dummyData = make([]byte, 100)
		// copy(dummyData, dummyTupleData1)
		log_rec = recovery.NewLogRecordNewPage(txn.GetTransactionId(), txn.GetPrevLSN(),
			recovery.NEWPAGE, types.PageID(cntup_num))
		lsn = lm.AppendLogRecord(log_rec)
		txn.SetPrevLSN(lsn)
		cntup_num++

		if cntup_num%100 == 0 {
			lm.Flush()
		}
	}

	//panic("serialize finish")

	readLogLoopCnt := 0
	deserializeLoopCnt := 0
	var file_offset uint32 = 0
	var readDataBytes uint32
	log_buffer := make([]byte, common.LogBufferSize)
	for dm.ReadLog(log_buffer, int32(file_offset), &readDataBytes) {
		var buffer_offset uint32 = 0
		var log_record recovery.LogRecord
		readLogLoopCnt++
		if readLogLoopCnt > 10 {
			//fmt.Printf("file_offset %d\n", file_offset)
			//fmt.Println(log_record)
			panic("readLogLoopCnt is illegal")
		}
		//fmt.Printf("outer file_offset %d\n", file_offset)
		for lr.DeserializeLogRecord(log_buffer[buffer_offset:readDataBytes], &log_record) {
			fmt.Printf("inner file_offset %d\n", file_offset)
			fmt.Printf("inner buffer_offset %d\n", buffer_offset)
			//fmt.Println(log_record)
			fmt.Println(log_record.Size)
			fmt.Println(log_record.Lsn)
			fmt.Println(log_record.Txn_id)
			fmt.Println(log_record.Prev_lsn)
			fmt.Println(log_record.Log_record_type)
			deserializeLoopCnt++
			// if deserializeLoopCnt > 10 {
			// 	//fmt.Println(log_record)
			// 	fmt.Printf("file_offset %d\n", file_offset)
			// 	fmt.Printf("buffer_offset %d\n", buffer_offset)
			// 	panic("deserializeLoopCnt is illegal")
			// }
			if log_record.Log_record_type == recovery.INSERT {
				fmt.Println("Deserialized INSERT log record.")
				fmt.Println(log_record.Insert_rid)
				fmt.Println(log_record.Insert_tuple)
			} else if log_record.Log_record_type == recovery.UPDATE {
				fmt.Println("Deserialized UPDATE log record.")
				fmt.Println(log_record.Update_rid)
				fmt.Println(log_record.Old_tuple.Size())
				fmt.Println(log_record.New_tuple.Size())
				// fmt.Println(log_record.Old_tuple)
				// fmt.Println(log_record.New_tuple)
			} else if log_record.Log_record_type == recovery.NEWPAGE {
				fmt.Println("Deserialized NEWPAGE log record.")
				fmt.Println(log_record.Prev_page_id)
			}
			buffer_offset += log_record.Size
		}
		// incomplete log record
		fmt.Printf("buffer_offset %d\n", buffer_offset)
		file_offset += buffer_offset
	}
}
*/

func TestRedo(t *testing.T) {
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
	col1 := column.NewColumn("a", types.Varchar, false)
	col2 := column.NewColumn("b", types.Integer, false)
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
	// TODO: (SDB) insert index entry if needed
	testingpkg.Assert(t, rid != nil, "")
	rid1, _ = test_table.InsertTuple(tuple1_, txn)
	// TODO: (SDB) insert index entry if needed
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
}

func TestUndo(t *testing.T) {
	os.Stdout.Sync()
	os.Remove("test.db")
	os.Remove("test.log")

	samehada_instance := test_util.NewSamehadaInstance()

	testingpkg.AssertFalse(t, common.EnableLogging, "")
	fmt.Println("Skip system recovering...")

	samehada_instance.GetLogManager().RunFlushThread()
	testingpkg.Assert(t, common.EnableLogging, "")
	//fmt.Println("System logging thread running...")

	fmt.Println("Create a test table")
	txn := samehada_instance.GetTransactionManager().Begin(nil)
	test_table := access.NewTableHeap(
		samehada_instance.GetBufferPoolManager(),
		samehada_instance.GetLogManager(),
		samehada_instance.GetLockManager(),
		txn)
	first_page_id := test_table.GetFirstPageId()

	col1 := column.NewColumn("a", types.Varchar, false)
	col2 := column.NewColumn("b", types.Integer, false)
	cols := []*column.Column{col1, col2}

	schema_ := schema.NewSchema(cols)
	tuple1 := ConstructTuple(schema_)
	val1_0 := tuple1.GetValue(schema_, 0)
	val1_1 := tuple1.GetValue(schema_, 1)
	var rid1 *page.RID
	rid1, _ = test_table.InsertTuple(tuple1, txn)
	testingpkg.Assert(t, rid1 != nil, "")

	tuple2 := ConstructTuple(schema_)
	val2_0 := tuple2.GetValue(schema_, 0)
	val2_1 := tuple2.GetValue(schema_, 1)
	fmt.Println(val2_0.ToVarchar())
	fmt.Println(val2_0.Size())
	fmt.Println(val2_1.ToInteger())

	var rid2 *page.RID
	rid2, _ = test_table.InsertTuple(tuple2, txn)
	testingpkg.Assert(t, rid2 != nil, "")

	fmt.Println("Log page content is written to disk")
	samehada_instance.GetLogManager().Flush()
	fmt.Println("two tuples inserted are commited")
	samehada_instance.GetTransactionManager().Commit(txn)

	txn = samehada_instance.GetTransactionManager().Begin(nil)

	// tuple deletion (rid1)
	test_table.MarkDelete(rid1, txn)

	// tuple updating (rid2)
	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewVarchar("updated"))
	row1 = append(row1, types.NewInteger(256))
	test_table.UpdateTuple(tuple.NewTupleFromSchema(row1, schema_), *rid2, txn)

	// tuple insertion (rid3)
	tuple3 := ConstructTuple(schema_)
	var rid3 *page.RID
	rid3, _ = test_table.InsertTuple(tuple3, txn)
	// TODO: (SDB) insert index entry if needed
	testingpkg.Assert(t, rid3 != nil, "")

	fmt.Println("Log page content is written to disk")
	samehada_instance.GetLogManager().Flush()
	fmt.Println("Table page content is written to disk")
	samehada_instance.GetBufferPoolManager().FlushPage(first_page_id)

	fmt.Println("System crash before commit")
	// delete samehada_instance
	samehada_instance.Finalize(false)

	fmt.Println("System restarted..")
	samehada_instance = test_util.NewSamehadaInstance()
	txn = samehada_instance.GetTransactionManager().Begin(nil)

	test_table = access.NewTableHeap(
		samehada_instance.GetBufferPoolManager(),
		samehada_instance.GetLogManager(),
		samehada_instance.GetLockManager(),
		txn)

	fmt.Println("Check if deleted tuple does not exist before recovery")
	old_tuple1 := test_table.GetTuple(rid1, txn)
	testingpkg.Assert(t, old_tuple1 == nil, "")

	fmt.Println("Check if updated tuple values are effected before recovery")
	var old_tuple2 *tuple.Tuple
	//testingpkg.Assert(t, test_table.GetTuple(rid, &old_tuple, txn), "")
	old_tuple2 = test_table.GetTuple(rid2, txn)
	testingpkg.Assert(t, old_tuple2 != nil, "")
	testingpkg.Assert(t, old_tuple2.GetValue(schema_, 0).CompareEquals(types.NewVarchar("updated")), "")
	testingpkg.Assert(t, old_tuple2.GetValue(schema_, 1).CompareEquals(types.NewInteger(256)), "")

	fmt.Println("Check if inserted tuple exists before recovery")
	old_tuple3 := test_table.GetTuple(rid3, txn)
	testingpkg.Assert(t, old_tuple3 != nil, "")

	testingpkg.AssertFalse(t, common.EnableLogging, "common.EnableLogging is not false!")

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

	//samehada_instance.GetTransactionManager().Commit(txn)

	fmt.Println("Check if failed txn is undo successfully")
	txn = samehada_instance.GetTransactionManager().Begin(nil)

	fmt.Println("Check deleted tuple exists")
	old_tuple1 = test_table.GetTuple(rid1, txn)
	testingpkg.Assert(t, old_tuple1 != nil, "")
	testingpkg.Assert(t, old_tuple1.GetValue(schema_, 0).CompareEquals(val1_0), "")
	testingpkg.Assert(t, old_tuple1.GetValue(schema_, 1).CompareEquals(val1_1), "")

	fmt.Println("Check updated tuple's values are rollbacked")
	old_tuple2 = test_table.GetTuple(rid2, txn)
	testingpkg.Assert(t, old_tuple2 != nil, "")
	fmt.Println(old_tuple2.GetValue(schema_, 0).ToVarchar())
	fmt.Println(old_tuple2.GetValue(schema_, 1).ToInteger())
	fmt.Println(val2_0.ToVarchar())
	fmt.Println(val2_1.ToInteger())
	testingpkg.Assert(t, old_tuple2.GetValue(schema_, 0).CompareEquals(val2_0), "")
	testingpkg.Assert(t, old_tuple2.GetValue(schema_, 1).CompareEquals(val2_1), "")

	fmt.Println("Check inserted tuple does not exist")
	old_tuple3 = test_table.GetTuple(rid3, txn)
	testingpkg.Assert(t, old_tuple3 == nil, "")

	//samehada_instance.GetTransactionManager().Commit(txn)

	fmt.Println("Tearing down the system..")
	samehada_instance.Finalize(true)
}

func TestCheckpoint(t *testing.T) {
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
	samehada_instance.GetTransactionManager().Commit(txn)

	col1 := column.NewColumn("a", types.Varchar, false)
	col2 := column.NewColumn("b", types.Integer, false)
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
		var rid *page.RID = nil
		rid, _ = test_table.InsertTuple(tuple_, txn1)
		// TODO: (SDB) insert index entry if needed
		testingpkg.Assert(t, rid != nil, "")
	}
	samehada_instance.GetTransactionManager().Commit(txn1)

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
			dmgr_impl := *samehada_instance.GetDiskManager()
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
	//EXPECT_EQ(persistent_lsn, (next_lsn - 1))
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
	samehada_instance.Finalize(true)

	fmt.Println("Tearing down the system..")
}

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
			len_ := int(1 + rand.Intn(math.MaxUint32)%10)
			//char s[10]
			s := ""
			for j := 0; j < len_; j++ {
				//s[j] = alphanum[rand.Intn(math.MaxUint32) % (sizeof(alphanum) - 1)]
				idx := rand.Intn(52)
				s = s + alphabets[idx:idx+1]
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

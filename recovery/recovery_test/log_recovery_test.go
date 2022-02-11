package log_recovery

import (
	"fmt"
	"testing"

	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/test_util"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
)

func TestRedo(t *testing.T) {
	// remove("test.db")
	// remove("test.log")

	samehada_instance := test_util.NewSamehadaInstance("test.db")

	testingpkg.AssertFalse(t, common.EnableLogging)
	fmt.Println("Skip system recovering...")

	samehada_instance.GetLogManager().RunFlushThread()
	testingpkg.Assert(t, common.EnableLogging)
	fmt.Println("System logging thread running...")

	fmt.Println("Create a test table")
	txn = samehada_instance.GetTransactionManager().Begin()
	test_table = access.NewTableHeap(samehada_instance.GetBufferPoolManager(), samehada_instance.lock_manager_,
		samehada_instance.GetLogManager(), txn)
	first_page_id := test_table.GetFirstPageId()

	var rid page.RID
	var rid1 page.RID
	col1 := &colmun.Column{"a", types.Varchar, 20}
	col2 := &colmun.Column{"b", types.Smallint}
	cols := []colmun.Column{col1, col2}
	schema_ := schema.NewSchema(cols)
	tuple_ := ConstructTuple(&schema_)
	tuple1_ := ConstructTuple(&schema_)

	val_1 := tuple_.GetValue(&schema_, 1)
	val_0 := tuple_.GetValue(&schema_, 0)
	val1_1 = tuple1_.GetValue(&schema_, 1)
	val1_0 = tuple1_.GetValue(&schema_, 0)

	testingpkg.Assert(t, test_table.InsertTuple(tuple_, &rid, txn))
	testingpkg.Assert(t, test_table.InsertTuple(tuple_1, &rid1, txn))

	samehada_instance.GetTransactionManager().Commit(txn)
	fmt.Println("Commit txn")

	// delete txn
	// delete test_table

	fmt.Println("Shutdown System")
	// delete samehada_instance

	fmt.Println("System restart...")
	samehada_instance := test_util.NewSamehadaInstance("test.db")

	testingpkg.AssertFalse(t, common.EnableLogging)
	fmt.Println("Check if tuple is not in table before recovery")
	var old_tuple tuple.Tuple
	var old_tuple1 tuple.Tuple
	txn := samehada_instance.GetTransactionManager().Begin()
	test_table := access.NewTableHeap(samehada_instance.GetBufferPoolManager(), samehada_instance.lock_manager_,
		samehada_instance.GetLogManager(), first_page_id)
	testingpkg.AssertFalse(t, test_table.GetTuple(rid, &old_tuple, txn), "")
	testingpkg.AssertFalse(t, test_table.GetTuple(rid1, &old_tuple1, txn), "")
	samehada_instance.GetTransactionManager().Commit(txn)
	// delete txn

	fmt.Println("Begin recovery")
	log_recovery := LogRecovery(samehada_instance.GetDiskManager(), samehada_instance.GetBufferPoolManager())

	testingpkg.AssertFalse(t, common.EnableLogging)

	fmt.Println("Redo underway...")
	log_recovery.Redo()
	fmt.Println("Undo underway...")
	log_recovery.Undo()

	fmt.Println("Check if recovery success")
	txn = samehada_instance.GetTransactionManager().Begin()
	// delete test_table
	test_table = access.NewTableHeap(samehada_instance.GetBufferPoolManager(), samehada_instance.lock_manager_,
		samehada_instance.GetLogManager(), first_page_id)

	testingpkg.Assert(t, test_table.GetTuple(rid, &old_tuple, txn), "")
	testingpkg.Assert(t, test_table.GetTuple(rid1, &old_tuple1, txn), "")
	samehada_instance.GetTransactionManager().Commit(txn)
	// delete txn
	// delete test_table
	// delete log_recovery

	// testingpkg.Equals(t, old_tuple.GetValue(&schema_, 1).CompareEquals(val_1), CmpBool::CmpTrue)
	// testingpkg.Equals(t, old_tuple.GetValue(&schema_, 0).CompareEquals(val_0), CmpBool::CmpTrue)
	// testingpkg.Equals(t, old_tuple1.GetValue(&schema_, 1).CompareEquals(val1_1), CmpBool::CmpTrue)
	// testingpkg.Equals(t, old_tuple1.GetValue(&schema_, 0).CompareEquals(val1_0), CmpBool::CmpTrue)
	testingpkg.Assert(t, old_tuple.GetValue(&schema_, 1).CompareEquals(val_1), "")
	testingpkg.Assert(t, old_tuple.GetValue(&schema_, 0).CompareEquals(val_0), "")
	testingpkg.Assert(t, old_tuple1.GetValue(&schema_, 1).CompareEquals(val1_1), "")
	testingpkg.Assert(t, old_tuple1.GetValue(&schema_, 0).CompareEquals(val1_0), "")

	// delete samehada_instance
	fmt.Println("Tearing down the system..")
	// remove("test.db")
	// remove("test.log")
}

func TestUndo() {
	remove("test.db")
	remove("test.log")
	samehada_instance = test_util.NewSamehadaInstance("test.db")

	testingpkg.AssertFalse(t, common.EnableLogging)
	fmt.Println("Skip system recovering...")

	samehada_instance.GetLogManager().RunFlushThread()
	testingpkg.Assert(t, common.EnableLogging)
	fmt.Println("System logging thread running...")

	fmt.Println("Create a test table")
	txn = samehada_instance.GetTransactionManager().Begin()
	test_table := access.NewTableHeap(samehada_instance.GetBufferPoolManager(), samehada_instance.lock_manager_,
		samehada_instance.GetLogManager(), txn)
	first_page_id := test_table.GetFirstPageId()

	col1 := &colmun.Column{"a", types.Varchar, 20}
	col2 := &colmun.Column{"b", types.Smallint}
	cols := []colmun.Column{col1, col2}
	schema_ := schema.NewSchema(cols)
	var rid page.RID
	tuple_ := ConstructTuple(&schema_)

	val_0 := tuple_.GetValue(&schema_, 0)
	val_1 := tuple_.GetValue(&schema_, 1)

	testingpkg.Assert(t, test_table.InsertTuple(tuple_, &rid, txn), "")

	fmt.Println("Table page content is written to disk")
	samehada_instance.GetBufferPoolManager().FlushPage(first_page_id)

	// delete txn
	// delete test_table

	fmt.Println("System crash before commit")
	// delete samehada_instance

	fmt.Println("System restarted..")
	samehada_instance := test_util.NewSamehadaInstance("test.db")

	fmt.Println("Check if tuple exists before recovery")
	var old_tuple tuple.Tuple
	txn := samehada_instance.GetTransactionManager().Begin()
	test_table := access.NewTableHeap(samehada_instance.GetBufferPoolManager(), samehada_instance.lock_manager_,
		samehada_instance.GetLogManager(), first_page_id)

	testingpkg.Assert(t, test_table.GetTuple(rid, &old_tuple, txn), "")
	//testingpkg.Equals(t, old_tuple.GetValue(&schema_, 0).CompareEquals(val_0), CmpBool::CmpTrue)
	testingpkg.Assert(t, old_tuple.GetValue(&schema_, 0).CompareEquals(val_0), "")
	//testingpkg.Assert(t, old_tuple.GetValue(&schema_, 1).CompareEquals(val_1), CmpBool::CmpTrue)
	testingpkg.Assert(t, old_tuple.GetValue(&schema_, 1).CompareEquals(val_1), "")
	samehada_instance.GetTransactionManager().Commit(txn)
	// delete txn

	fmt.Println("Recovery started..")
	log_recovery = LogRecovery(samehada_instance.GetDiskManager(), samehada_instance.GetBufferPoolManager())

	testingpkg.AssertFalse(t, common.EnableLogging, "")

	log_recovery.Redo()
	fmt.Println("Redo underway...")
	log_recovery.Undo()
	fmt.Println("Undo underway...")

	fmt.Println("Check if failed txn is undo successfully")
	txn = samehada_instance.GetTransactionManager().Begin()
	// delete test_table
	test_table = access.NewTableHeap(samehada_instance.GetBufferPoolManager(), samehada_instance.lock_manager_,
		samehada_instance.GetLogManager(), first_page_id)

	testingpkg.AssertFalse(t, test_table.GetTuple(rid, &old_tuple, txn), "")
	samehada_instance.GetTransactionManager().Commit(txn)

	// delete txn
	// delete test_table
	// delete log_recovery

	// delete samehada_instance
	fmt.Println("Tearing down the system..")
	// remove("test.db")
	// remove("test.log")
}

// TODO: (SDB) need prepare "expect" type testing utility func
func TestCheckpoint() {
	// remove("test.db")
	// remove("test.log")
	samehada_instance := test_util.NewSamehadaInstance("test.db")

	EXPECT_FALSE(common.EnableLogging)
	fmt.Println("Skip system recovering...")

	samehada_instance.GetLogManager().RunFlushThread()
	EXPECT_TRUE(common.EnableLogging)
	fmt.Println("System logging thread running...")

	fmt.Println("Create a test table")
	txn := samehada_instance.GetTransactionManager().Begin()
	test_table := access.NewTableHeap(samehada_instance.GetBufferPoolManager(), samehada_instance.lock_manager_,
		samehada_instance.GetLogManager(), txn)
	samehada_instance.GetTransactionManager().Commit(txn)

	col1 := &colmun.Column{"a", types.Varchar, 20}
	col2 := &colmun.Column{"b", types.Smallint}
	cols := []colmun.Column{col1, col2}
	schema_ := schema.NewSchema(cols)

	tuple_ := ConstructTuple(&schema_)
	val_0 := tuple_.GetValue(&schema_, 0)
	val_1 := tuple_.GetValue(&schema_, 1)

	// set log time out very high so that flush doesn't happen before checkpoint is performed
	log_timeout := seconds(15) //chrono::seconds(15)

	// insert a ton of tuples
	txn1 := samehada_instance.GetTransactionManager().Begin()
	for i := 0; i < 1000; i++ {
		var rid page.RID
		EXPECT_TRUE(test_table.InsertTuple(tuple_, &rid, txn1))
	}
	samehada_instance.GetTransactionManager().Commit(txn1)

	// Do checkpoint
	samehada_instance.checkpoint_manager_.BeginCheckpoint()
	samehada_instance.checkpoint_manager_.EndCheckpoint()

	pages := samehada_instance.GetBufferPoolManager().GetPages()
	pool_size := samehada_instance.GetBufferPoolManager().GetPoolSize()

	// make sure that all pages in the buffer pool are marked as non-dirty
	all_pages_clean := true
	for i := 0; i < pool_size; i++ {
		page := &pages[i]
		page_id := page.GetPageId()

		if page_id != INVALID_PAGE_ID && page.IsDirty() {
			all_pages_clean = false
			break
		}
	}
	EXPECT_TRUE(all_pages_clean)

	// compare each page in the buffer pool to that page's
	// data on disk. ensure they match after the checkpoint
	all_pages_match := true
	disk_data := char[PAGE_SIZE]
	for i := 0; i < pool_size; i++ {
		page := &pages[i]
		page_id := page.GetPageId()

		if page_id != INVALID_PAGE_ID {
			samehada_instance.GetDiskManager().ReadPage(page_id, disk_data)
			if memcmp(disk_data, page.GetData(), PAGE_SIZE) != 0 {
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
		page := &pages[i]
		page_id := page.GetPageId()

		if page_id != INVALID_PAGE_ID && page.GetLSN() > persistent_lsn {
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

// TODO: (SDB) need implement for testing recovery and checkpoint mechanism
// use a fixed schema to construct a random tuple
func ConstructTuple(schema_ *schema.Schema) tuple.Tuple {
	/*
		std::vector<Value> values;
		Value v(TypeId::INVALID);

		auto seed = std::chrono::system_clock::now().time_since_epoch().count();

		std::mt19937 generator(seed);  // mt19937 is a standard mersenne_twister_engine

		for (uint32_t i = 0; i < schema_->GetColumnCount(); i++) {
		  // get type
		  const auto &col = schema_->GetColumn(i);
		  TypeId type = col.GetType();
		  switch (type) {
			case TypeId::BOOLEAN:
			  v = Value(type, static_cast<int8_t>(generator() % 2));
			  break;
			case TypeId::TINYINT:
			  v = Value(type, static_cast<int8_t>(generator()) % 1000);
			  break;
			case TypeId::SMALLINT:
			  v = Value(type, static_cast<int16_t>(generator()) % 1000);
			  break;
			case TypeId::INTEGER:
			  v = Value(type, static_cast<int32_t>(generator()) % 1000);
			  break;
			case TypeId::BIGINT:
			  v = Value(type, static_cast<int64_t>(generator()) % 1000);
			  break;
			case TypeId::VARCHAR: {
			  static const char alphanum[] =
				  "0123456789"
				  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
				  "abcdefghijklmnopqrstuvwxyz";
			  auto len = static_cast<uint32_t>(1 + generator() % 9);
			  char s[10];
			  for (uint32_t j = 0; j < len; ++j) {
				s[j] = alphanum[generator() % (sizeof(alphanum) - 1)];
			  }
			  s[len] = 0;
			  v = Value(type, s, len + 1, true);
			  break;
			}
			default:
			  break;
		  }
		  values.emplace_back(v);
		}
		return Tuple(values, schema_);
	*/
	return *tuple.NewTuple(new(page.RID), 0, []byte{})
}

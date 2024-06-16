// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package access

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	testingpkg "github.com/ryogrid/SamehadaDB/lib/testing/testing_assert"
	"math"
	"testing"

	"github.com/ryogrid/SamehadaDB/lib/recovery"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/disk"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/column"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

func TestTableHeap(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true

	dm := disk.NewDiskManagerTest()
	defer dm.ShutDown()
	log_manager := recovery.NewLogManager(&dm)
	bpm := buffer.NewBufferPoolManager(10, dm, log_manager)
	lock_manager := NewLockManager(STRICT, SS2PL_MODE)
	txn_mgr := NewTransactionManager(lock_manager, log_manager)
	txn := txn_mgr.Begin(nil)

	th := NewTableHeap(bpm, log_manager, lock_manager, txn)

	// this schema creates a tuple1 of size 8 bytes
	// it means that a page can only contains 254 tuples of this schema
	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	// inserting 1000 tuples, means that we need at least 5 pages to insert all tuples
	for i := 0; i < 1000; i++ {
		row := make([]types.Value, 0)
		row = append(row, types.NewInteger(int32(i*2)))
		row = append(row, types.NewInteger(int32((i+1)*2)))

		tuple_ := tuple.NewTupleFromSchema(row, schema_)
		_, err := th.InsertTuple(tuple_, txn, math.MaxUint32, false)
		testingpkg.Ok(t, err)
	}

	bpm.FlushAllPages()

	firstTuple := th.GetFirstTuple(txn)
	testingpkg.Equals(t, int32(0), firstTuple.GetValue(schema_, 0).ToInteger())
	testingpkg.Equals(t, int32(2), firstTuple.GetValue(schema_, 1).ToInteger())

	for i := 0; i < 1000; i++ {
		rid := &page.RID{}
		// (4096 - 24) / (8 + (5 * 2)) => 226.222...

		rid.Set(types.PageID(i/226), uint32(i%226))
		tuple, _ := th.GetTuple(rid, txn)
		testingpkg.Equals(t, int32(i*2), tuple.GetValue(schema_, 0).ToInteger())
		testingpkg.Equals(t, int32((i+1)*2), tuple.GetValue(schema_, 1).ToInteger())
	}

	// 4 pages should have the size of 4096 * 5 => 20480 bytes
	testingpkg.Equals(t, int64(20480), dm.Size())

	// let's iterate through the heap using the iterator
	it := th.Iterator(txn)
	i := int32(0)
	tuple_cnt := 0
	for tuple_ := it.Current(); !it.End(); tuple_ = it.Next() {
		testingpkg.Equals(t, i*2, tuple_.GetValue(schema_, 0).ToInteger())
		testingpkg.Equals(t, (i+1)*2, tuple_.GetValue(schema_, 1).ToInteger())
		i++
		tuple_cnt++
	}
	fmt.Println(tuple_cnt)
	testingpkg.Assert(t, tuple_cnt == 1000, "quontity of returned tuples differ one of expected.")

	txn_mgr.Commit(nil, txn)

	common.TempSuppressOnMemStorage = false
	common.TempSuppressOnMemStorageMutex.Unlock()
}

func TestTableHeapFourCol(t *testing.T) {
	dm := disk.NewDiskManagerTest()
	defer dm.ShutDown()
	log_manager := recovery.NewLogManager(&dm)
	bpm := buffer.NewBufferPoolManager(10, dm, log_manager)
	lock_manager := NewLockManager(STRICT, SS2PL_MODE)
	txn_mgr := NewTransactionManager(lock_manager, log_manager)
	txn := txn_mgr.Begin(nil)

	th := NewTableHeap(bpm, log_manager, lock_manager, txn)

	// this schema creates a tuple1 of size (4 + 1) * 4 => 20 bytes (when includes metadata at header the value is 28)
	// it means that a page can only contains 145 tuples of this schema
	// (4096 - 24) / 28 => 145.4285...
	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnC := column.NewColumn("c", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnD := column.NewColumn("d", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)

	schema_ := schema.NewSchema([]*column.Column{columnA, columnB, columnC, columnD})

	// inserting 1000 tuples, means that we need at least 7 pages to insert all tuples
	// 1000 / 145 => 6.896...
	for i := 0; i < 1000; i++ {
		row := make([]types.Value, 0)
		row = append(row, types.NewInteger(int32(i*2)))
		row = append(row, types.NewInteger(int32((i+1)*2)))
		row = append(row, types.NewInteger(int32((i+2)*2)))
		row = append(row, types.NewInteger(int32((i+3)*2)))

		tuple_ := tuple.NewTupleFromSchema(row, schema_)
		_, err := th.InsertTuple(tuple_, txn, math.MaxUint32, false)
		testingpkg.Ok(t, err)
	}

	bpm.FlushAllPages()

	firstTuple := th.GetFirstTuple(txn)
	testingpkg.Equals(t, int32(0), firstTuple.GetValue(schema_, 0).ToInteger())
	testingpkg.Equals(t, int32(2), firstTuple.GetValue(schema_, 1).ToInteger())
	testingpkg.Equals(t, int32(4), firstTuple.GetValue(schema_, 2).ToInteger())
	testingpkg.Equals(t, int32(6), firstTuple.GetValue(schema_, 3).ToInteger())

	for i := 0; i < 1000; i++ {
		rid := &page.RID{}
		rid.Set(types.PageID(i/145), uint32(i%145))
		tuple_, _ := th.GetTuple(rid, txn)
		testingpkg.Equals(t, int32(i*2), tuple_.GetValue(schema_, 0).ToInteger())
		testingpkg.Equals(t, int32((i+1)*2), tuple_.GetValue(schema_, 1).ToInteger())
		testingpkg.Equals(t, int32((i+2)*2), tuple_.GetValue(schema_, 2).ToInteger())
		testingpkg.Equals(t, int32((i+3)*2), tuple_.GetValue(schema_, 3).ToInteger())
	}

	// 7 pages should have the size of 4096 * 7 => 28672 bytes
	testingpkg.Equals(t, int64(28672), dm.Size())

	// let's iterate through the heap using the iterator
	it := th.Iterator(txn)
	i := int32(0)

	tuple_cnt := 0
	for tuple := it.Current(); !it.End(); tuple = it.Next() {
		testingpkg.Equals(t, i*2, tuple.GetValue(schema_, 0).ToInteger())
		testingpkg.Equals(t, (i+1)*2, tuple.GetValue(schema_, 1).ToInteger())
		testingpkg.Equals(t, (i+2)*2, tuple.GetValue(schema_, 2).ToInteger())
		testingpkg.Equals(t, (i+3)*2, tuple.GetValue(schema_, 3).ToInteger())
		i++
		tuple_cnt++
	}
	testingpkg.Assert(t, tuple_cnt == 1000, "quontity of returned tuples differ one of expected.")

	txn_mgr.Commit(nil, txn)
}

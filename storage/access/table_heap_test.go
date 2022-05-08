// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package access

import (
	"testing"

	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/disk"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
)

func TestTableHeap(t *testing.T) {
	dm := disk.NewDiskManagerTest()
	defer dm.ShutDown()
	log_manager := recovery.NewLogManager(&dm)
	bpm := buffer.NewBufferPoolManager(10, dm, log_manager)
	lock_manager := NewLockManager(STRICT, SS2PL_MODE)
	txn_mgr := NewTransactionManager(lock_manager, log_manager)
	//txn := NewTransaction(types.TxnID(0))
	txn := txn_mgr.Begin(nil)

	th := NewTableHeap(bpm, log_manager, lock_manager, txn)

	// this schema creates a tuple of size 8 bytes
	// it means that a page can only contains 254 tuples of this schema
	columnA := column.NewColumn("a", types.Integer, false, nil)
	columnB := column.NewColumn("b", types.Integer, false, nil)
	schema := schema.NewSchema([]*column.Column{columnA, columnB})

	// inserting 1000 tuples, means that we need at least 4 pages to insert all tuples
	for i := 0; i < 1000; i++ {
		row := make([]types.Value, 0)
		row = append(row, types.NewInteger(int32(i*2)))
		row = append(row, types.NewInteger(int32((i+1)*2)))

		tuple := tuple.NewTupleFromSchema(row, schema)
		_, err := th.InsertTuple(tuple, txn)
		testingpkg.Ok(t, err)
	}

	bpm.FlushAllPages()

	firstTuple := th.GetFirstTuple(txn)
	testingpkg.Equals(t, int32(0), firstTuple.GetValue(schema, 0).ToInteger())
	testingpkg.Equals(t, int32(2), firstTuple.GetValue(schema, 1).ToInteger())

	for i := 0; i < 1000; i++ {
		rid := &page.RID{}
		rid.Set(types.PageID(i/254), uint32(i%254))
		tuple := th.GetTuple(rid, txn)
		testingpkg.Equals(t, int32(i*2), tuple.GetValue(schema, 0).ToInteger())
		testingpkg.Equals(t, int32((i+1)*2), tuple.GetValue(schema, 1).ToInteger())
	}

	// 4 pages should have the size of 16384 bytes
	testingpkg.Equals(t, int64(16384), dm.Size())

	// let's iterate through the heap using the iterator
	it := th.Iterator(txn)
	i := int32(0)
	for tuple := it.Current(); !it.End(); tuple = it.Next() {
		testingpkg.Equals(t, i*2, tuple.GetValue(schema, 0).ToInteger())
		testingpkg.Equals(t, (i+1)*2, tuple.GetValue(schema, 1).ToInteger())
		i++
	}

	txn_mgr.Commit(txn)
}

func TestTableHeapFourCol(t *testing.T) {
	dm := disk.NewDiskManagerTest()
	defer dm.ShutDown()
	log_manager := recovery.NewLogManager(&dm)
	bpm := buffer.NewBufferPoolManager(10, dm, log_manager)
	lock_manager := NewLockManager(STRICT, SS2PL_MODE)
	txn_mgr := NewTransactionManager(lock_manager, log_manager)
	//txn := NewTransaction(types.TxnID(0))
	txn := txn_mgr.Begin(nil)

	th := NewTableHeap(bpm, log_manager, lock_manager, txn)

	// this schema creates a tuple of size 8 bytes
	// it means that a page can only contains 254 tuples of this schema
	columnA := column.NewColumn("a", types.Integer, false, nil)
	columnB := column.NewColumn("b", types.Integer, false, nil)
	columnC := column.NewColumn("c", types.Integer, false, nil)
	columnD := column.NewColumn("d", types.Integer, false, nil)

	schema := schema.NewSchema([]*column.Column{columnA, columnB, columnC, columnD})

	// inserting 1000 tuples, means that we need at least 8 pages to insert all tuples
	for i := 0; i < 1000; i++ {
		row := make([]types.Value, 0)
		row = append(row, types.NewInteger(int32(i*2)))
		row = append(row, types.NewInteger(int32((i+1)*2)))
		row = append(row, types.NewInteger(int32((i+2)*2)))
		row = append(row, types.NewInteger(int32((i+3)*2)))

		tuple := tuple.NewTupleFromSchema(row, schema)
		_, err := th.InsertTuple(tuple, txn)
		testingpkg.Ok(t, err)
	}

	bpm.FlushAllPages()

	firstTuple := th.GetFirstTuple(txn)
	testingpkg.Equals(t, int32(0), firstTuple.GetValue(schema, 0).ToInteger())
	testingpkg.Equals(t, int32(4), firstTuple.GetValue(schema, 1).ToInteger())

	for i := 0; i < 1000; i++ {
		rid := &page.RID{}
		rid.Set(types.PageID(i/254), uint32(i%254))
		tuple := th.GetTuple(rid, txn)
		testingpkg.Equals(t, int32(i*2), tuple.GetValue(schema, 0).ToInteger())
		testingpkg.Equals(t, int32((i+1)*2), tuple.GetValue(schema, 1).ToInteger())
		testingpkg.Equals(t, int32((i+2)*2), tuple.GetValue(schema, 2).ToInteger())
		testingpkg.Equals(t, int32((i+3)*2), tuple.GetValue(schema, 3).ToInteger())
	}

	//// 8 pages should have the size of 4096 * 8 bytes
	testingpkg.Equals(t, int64(4094*8), dm.Size())

	// let's iterate through the heap using the iterator
	it := th.Iterator(txn)
	i := int32(0)
	for tuple := it.Current(); !it.End(); tuple = it.Next() {
		testingpkg.Equals(t, i*2, tuple.GetValue(schema, 0).ToInteger())
		testingpkg.Equals(t, (i+1)*2, tuple.GetValue(schema, 1).ToInteger())
		testingpkg.Equals(t, (i+2)*2, tuple.GetValue(schema, 2).ToInteger())
		testingpkg.Equals(t, (i+3)*2, tuple.GetValue(schema, 3).ToInteger())
		i++
	}

	txn_mgr.Commit(txn)
}

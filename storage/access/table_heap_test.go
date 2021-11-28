// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in license/go-bustub dir

package access

import (
	"testing"

	"github.com/ryogrid/SaitomDB/storage/buffer"
	"github.com/ryogrid/SaitomDB/storage/disk"
	"github.com/ryogrid/SaitomDB/storage/page"
	"github.com/ryogrid/SaitomDB/storage/table"
	testingpkg "github.com/ryogrid/SaitomDB/testing"
	"github.com/ryogrid/SaitomDB/types"
)

func TestTableHeap(t *testing.T) {
	dm := disk.NewDiskManagerTest()
	defer dm.ShutDown()
	bpm := buffer.NewBufferPoolManager(10, dm)

	th := NewTableHeap(bpm)

	// this schema creates a tuple of size 8 bytes
	// it means that a page can only contains 254 tuples of this schema
	columnA := table.NewColumn("a", types.Integer)
	columnB := table.NewColumn("b", types.Integer)
	schema := table.NewSchema([]*table.Column{columnA, columnB})

	// inserting 1000 tuples, means that we need at least 4 pages to insert all tuples
	for i := 0; i < 1000; i++ {
		row := make([]types.Value, 0)
		row = append(row, types.NewInteger(int32(i*2)))
		row = append(row, types.NewInteger(int32((i+1)*2)))

		tuple := table.NewTupleFromSchema(row, schema)
		_, err := th.InsertTuple(tuple)
		testingpkg.Ok(t, err)
	}

	bpm.FlushAllpages()

	firstTuple := th.GetFirstTuple()
	testingpkg.Equals(t, int32(0), firstTuple.GetValue(schema, 0).ToInteger())
	testingpkg.Equals(t, int32(2), firstTuple.GetValue(schema, 1).ToInteger())

	for i := 0; i < 1000; i++ {
		rid := &page.RID{}
		rid.Set(types.PageID(i/254), uint32(i%254))
		tuple := th.GetTuple(rid)
		testingpkg.Equals(t, int32(i*2), tuple.GetValue(schema, 0).ToInteger())
		testingpkg.Equals(t, int32((i+1)*2), tuple.GetValue(schema, 1).ToInteger())
	}

	// 4 pages should have the size of 16384 bytes
	testingpkg.Equals(t, int64(16384), dm.Size())

	// let's iterate through the heap using the iterator
	it := th.Iterator()
	i := int32(0)
	for tuple := it.Current(); !it.End(); tuple = it.Next() {
		testingpkg.Equals(t, i*2, tuple.GetValue(schema, 0).ToInteger())
		testingpkg.Equals(t, (i+1)*2, tuple.GetValue(schema, 1).ToInteger())
		i++
	}
}

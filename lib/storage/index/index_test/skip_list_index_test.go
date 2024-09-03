package index

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/samehada"
	"github.com/ryogrid/SamehadaDB/lib/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/column"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	testingpkg "github.com/ryogrid/SamehadaDB/lib/testing/testing_assert"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"math/rand"
	"os"
	"testing"
)

func testKeyDuplicateInsDelSkipListIndex[T float32 | int32 | string](t *testing.T, keyType types.TypeID) {
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

	columnA := column.NewColumn("test", keyType, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA})
	tableMetadata := c.CreateTable("test_1", schema_, txn)

	txnMgr.Commit(c, txn)

	var duplicatedVal interface{}
	switch keyType {
	case types.Integer:
		duplicatedVal = int32(10)
	case types.Float:
		duplicatedVal = float32(-5.2)
	case types.Varchar:
		duplicatedVal = "duplicateTest"
	default:
		panic("unsuppoted value type")
	}

	tuple_ := tuple.NewTupleFromSchema([]types.Value{types.NewValue(duplicatedVal)}, schema_)
	rid1 := page.RID{10, 0}
	rid2 := page.RID{11, 1}
	rid3 := page.RID{12, 2}

	indexTest := tableMetadata.GetIndex(0)

	indexTest.InsertEntry(tuple_, rid1, txn)
	indexTest.InsertEntry(tuple_, rid2, txn)
	indexTest.InsertEntry(tuple_, rid3, txn)

	result := indexTest.ScanKey(tuple_, txn)
	fmt.Println(result[0], result[1], result[2])
	testingpkg.Assert(t, len(result) == 3, "duplicated key point scan got illegal results.")
	indexTest.DeleteEntry(tuple_, rid1, txn)
	result = indexTest.ScanKey(tuple_, txn)
	fmt.Println(result[0], result[1])
	testingpkg.Assert(t, len(result) == 2, "duplicated key point scan got illegal results.")
	indexTest.DeleteEntry(tuple_, rid2, txn)
	result = indexTest.ScanKey(tuple_, txn)
	fmt.Println(result[0])
	testingpkg.Assert(t, len(result) == 1, "duplicated key point scan got illegal results.")
	indexTest.DeleteEntry(tuple_, rid3, txn)
	result = indexTest.ScanKey(tuple_, txn)
	testingpkg.Assert(t, len(result) == 0, "duplicated key point scan got illegal results.")
}

func testKeyDuplicateInsDelSkipListIndex2Col[T float32 | int32 | string](t *testing.T, keyType types.TypeID) {
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

	columnA := column.NewColumn("col1", keyType, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	columnB := column.NewColumn("col2", keyType, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})
	tableMetadata := c.CreateTable("test", schema_, txn)

	txnMgr.Commit(c, txn)

	var duplicatedVal interface{}
	switch keyType {
	case types.Integer:
		duplicatedVal = int32(10)
	case types.Float:
		duplicatedVal = float32(-5.2)
	case types.Varchar:
		duplicatedVal = "duplicateTest"
	default:
		panic("unsuppoted value type")
	}

	var getUniqVal = func(keyType types.TypeID) interface{} {
		var uniqueVal interface{}
		switch keyType {
		case types.Integer:
			uniqueVal = rand.Int31()
		case types.Float:
			uniqueVal = rand.Float32()
		case types.Varchar:
			uniqueVal = samehada_util.GetRandomStr(10, false)
		default:
			panic("unsuppoted value type")
		}
		return uniqueVal
	}

	// col1's values are same all (duplicated). col2's values has no duplication.
	tuple1 := tuple.NewTupleFromSchema([]types.Value{types.NewValue(duplicatedVal), types.NewValue(getUniqVal(keyType))}, schema_)
	tuple2 := tuple.NewTupleFromSchema([]types.Value{types.NewValue(duplicatedVal), types.NewValue(getUniqVal(keyType))}, schema_)
	tuple3 := tuple.NewTupleFromSchema([]types.Value{types.NewValue(duplicatedVal), types.NewValue(getUniqVal(keyType))}, schema_)

	rid1 := page.RID{10, 0}
	rid2 := page.RID{11, 1}
	rid3 := page.RID{12, 2}

	indexTest1 := tableMetadata.GetIndex(0)
	indexTest2 := tableMetadata.GetIndex(1)

	indexTest1.InsertEntry(tuple1, rid1, txn)
	indexTest1.InsertEntry(tuple2, rid2, txn)
	indexTest1.InsertEntry(tuple3, rid3, txn)
	indexTest2.InsertEntry(tuple1, rid1, txn)
	indexTest2.InsertEntry(tuple2, rid2, txn)
	indexTest2.InsertEntry(tuple3, rid3, txn)

	// check Index of column which has duplicated values
	result := indexTest1.ScanKey(tuple1, txn)
	fmt.Println(result[0], result[1], result[2])
	testingpkg.Assert(t, len(result) == 3, "duplicated key point scan got illegal results.")
	indexTest1.DeleteEntry(tuple1, rid1, txn)
	result = indexTest1.ScanKey(tuple1, txn)
	fmt.Println(result[0], result[1])
	testingpkg.Assert(t, len(result) == 2, "duplicated key point scan got illegal results.")
	indexTest1.DeleteEntry(tuple1, rid2, txn)
	result = indexTest1.ScanKey(tuple1, txn)
	fmt.Println(result[0])
	testingpkg.Assert(t, len(result) == 1, "duplicated key point scan got illegal results.")
	indexTest1.DeleteEntry(tuple1, rid3, txn)
	result = indexTest1.ScanKey(tuple1, txn)
	testingpkg.Assert(t, len(result) == 0, "duplicated key point scan got illegal results.")

	// check Index of column which has no duplicated value
	result = indexTest2.ScanKey(tuple1, txn)
	fmt.Println(result[0])
	testingpkg.Assert(t, len(result) == 1, "unique key point scan got illegal results.")
	indexTest2.DeleteEntry(tuple1, rid1, txn)
	result = indexTest1.ScanKey(tuple1, txn)
	testingpkg.Assert(t, len(result) == 0, "unique key point scan got illegal results.")
	result = indexTest2.ScanKey(tuple2, txn)
	fmt.Println(result[0])
	testingpkg.Assert(t, len(result) == 1, "unique key point scan got illegal results.")
	indexTest2.DeleteEntry(tuple2, rid2, txn)
	result = indexTest1.ScanKey(tuple2, txn)
	testingpkg.Assert(t, len(result) == 0, "unique key point scan got illegal results.")
	result = indexTest2.ScanKey(tuple3, txn)
	fmt.Println(result[0])
	testingpkg.Assert(t, len(result) == 1, "unique key point scan got illegal results.")
	indexTest2.DeleteEntry(tuple3, rid3, txn)
	result = indexTest1.ScanKey(tuple3, txn)
	testingpkg.Assert(t, len(result) == 0, "unique key point scan got illegal results.")
}

func TestKeyDuplicateInsDelSkipListIndexInt(t *testing.T) {
	testKeyDuplicateInsDelSkipListIndex[int32](t, types.Integer)
}

func TestKeyDuplicateInsDelSkipListIndex2ColInt(t *testing.T) {
	testKeyDuplicateInsDelSkipListIndex2Col[int32](t, types.Integer)
}

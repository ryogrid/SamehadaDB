package index_test

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

func TestBTreeIndexKeyDuplicateInsertDeleteSerialInt(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}

	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	keyType := types.Integer

	shi := samehada.NewSamehadaInstance(t.Name(), 500)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")
	txnMgr := shi.GetTransactionManager()

	txn := txnMgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)

	columnA := column.NewColumn("col1", keyType, true, index_constants.INDEX_KIND_BTREE, types.PageID(-1), nil)
	columnB := column.NewColumn("col2", keyType, true, index_constants.INDEX_KIND_BTREE, types.PageID(-1), nil)
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
			uniqueVal = samehada_util.GetRandomStr(10)
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

func TestBTreeIndexKeyDuplicateInsertDeleteStrideSerialInt(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}

	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	keyType := types.Integer
	stride := 400
	opTimes := 100
	seed := 2024
	r := rand.New(rand.NewSource(int64(seed)))

	//shi := samehada.NewSamehadaInstance(t.Name(), 500)
	shi := samehada.NewSamehadaInstance(t.Name(), 1000)

	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")
	txnMgr := shi.GetTransactionManager()

	txn := txnMgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)

	columnA := column.NewColumn("col1", keyType, true, index_constants.INDEX_KIND_BTREE, types.PageID(-1), nil)
	columnB := column.NewColumn("col2", keyType, true, index_constants.INDEX_KIND_BTREE, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})
	tableMetadata := c.CreateTable("test", schema_, txn)

	txnMgr.Commit(c, txn)

	//var getUniqVal = func(r *rand.Rand, keyType types.TypeID) interface{} {
	//	var uniqueVal interface{}
	//	switch keyType {
	//	case types.Integer:
	//		uniqueVal = r.Int31()
	//	case types.Float:
	//		uniqueVal = r.Float32()
	//	case types.Varchar:
	//		uniqueVal = samehada_util.GetRandomStr(10)
	//	default:
	//		panic("unsuppoted value type")
	//	}
	//	return uniqueVal
	//}

	indexTest1 := tableMetadata.GetIndex(0)
	indexTest2 := tableMetadata.GetIndex(1)

	for ii := 0; ii < opTimes; ii++ {
		fmt.Println("Insertion stride:", ii)
		for jj := 0; jj < stride; jj++ {
			// col1's values are same all (duplicated). col2's values has no duplication.
			tuple1 := tuple.NewTupleFromSchema([]types.Value{types.NewValue(int32(ii*stride + jj)), types.NewValue(int32(ii*stride + jj))}, schema_)
			tuple2 := tuple.NewTupleFromSchema([]types.Value{types.NewValue(int32(ii*stride + jj)), types.NewValue(int32(ii*stride + jj))}, schema_)
			tuple3 := tuple.NewTupleFromSchema([]types.Value{types.NewValue(int32(ii*stride + jj)), types.NewValue(int32(ii*stride + jj))}, schema_)

			rid1 := page.RID{types.PageID(ii*stride + jj), uint32(ii*stride + jj)}
			rid2 := page.RID{types.PageID(ii*stride + jj + 1), uint32(ii*stride + jj + 1)}
			rid3 := page.RID{types.PageID(ii*stride + jj + 2), uint32(ii*stride + jj + 2)}

			indexTest1.InsertEntry(tuple1, rid1, nil)
			indexTest1.InsertEntry(tuple2, rid2, nil)
			indexTest1.InsertEntry(tuple3, rid3, nil)

			indexTest2.InsertEntry(tuple1, rid1, nil)
			indexTest2.InsertEntry(tuple2, rid2, nil)
			indexTest2.InsertEntry(tuple3, rid3, nil)
		}
	}

	fmt.Println("Insertion done.")

	// make arr for roop index
	strideBaseArr := make([]int, opTimes)
	for ii := 0; ii < opTimes; ii++ {
		strideBaseArr[ii] = ii
	}

	// shuffle 0-(opTimes-1)
	r.Shuffle(opTimes, func(i, j int) {
		strideBaseArr[i], strideBaseArr[j] = strideBaseArr[j], strideBaseArr[i]
	})

	// check Index of column which has duplicated values
	// with random order
	cnt := 0
	for _, idx := range strideBaseArr {
		fmt.Println("Checking stride:", cnt)
		cnt++
		for jj := 0; jj < stride; jj++ {
			// index1
			tuple1 := tuple.NewTupleFromSchema([]types.Value{types.NewValue(int32(idx*stride + jj)), types.NewValue(int32(idx*stride + jj))}, schema_)
			result1 := indexTest1.ScanKey(tuple1, nil)
			//testingpkg.Assert(t, result1[0].PageId == types.PageID(idx*stride+jj) && result1[0].SlotNum == uint32(idx*stride+jj), fmt.Sprintf("duplicated key point scan got illegal value.(1) idx: %d, jj: %d", idx, jj))
			//testingpkg.Assert(t, len(result1) == 3, fmt.Sprintf("duplicated key point scan got illegal results.(1) idx: %d, jj: %d len(result1): %d", idx, jj, len(result1)))
			testingpkg.Assert(t, len(result1) != 0, fmt.Sprintf("duplicated key point scan got illegal results.(1) idx: %d, jj: %d len(result1): %d", idx, jj, len(result1)))
			//for _, res := range result1 {
			//	indexTest1.DeleteEntry(tuple1, res, nil)
			//}
			//result1 = indexTest1.ScanKey(tuple1, nil)
			//testingpkg.Assert(t, len(result1) == 0, "deleted key point scan got illegal results. (1)")

			// index2
			tuple2 := tuple.NewTupleFromSchema([]types.Value{types.NewValue(int32(idx*stride + jj)), types.NewValue(int32(idx*stride + jj))}, schema_)
			result2 := indexTest2.ScanKey(tuple2, nil)
			//testingpkg.Assert(t, len(result2) == 3, fmt.Sprintf("duplicated key point scan got illegal results.(2) idx: %d, jj: %d len(result1): %d", idx, jj, len(result2)))
			testingpkg.Assert(t, len(result2) != 0, fmt.Sprintf("duplicated key point scan got illegal results.(2) idx: %d, jj: %d len(result1): %d", idx, jj, len(result2)))
			//for _, res := range result2 {
			//	indexTest2.DeleteEntry(tuple2, res, nil)
			//}
			//result2 = indexTest2.ScanKey(tuple2, nil)
			//testingpkg.Assert(t, len(result2) == 0, fmt.Sprintf("deleted key point scan got illegal results. (2) idx: %d jj: %d len(result1): %d", idx, jj, len(result1)))
		}
	}

}

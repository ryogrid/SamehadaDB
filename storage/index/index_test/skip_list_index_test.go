package index

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/samehada"
	"github.com/ryogrid/SamehadaDB/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
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

func TestKeyDuplicateInsDelSkipListIndexInt(t *testing.T) {
	testKeyDuplicateInsDelSkipListIndex[int32](t, types.Integer)
}

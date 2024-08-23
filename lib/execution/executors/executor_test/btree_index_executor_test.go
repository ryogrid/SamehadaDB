package executor_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/samehada"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/column"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	testingpkg "github.com/ryogrid/SamehadaDB/lib/testing/testing_assert"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"os"
	"testing"
)

func testKeyDuplicateInsertDeleteWithBTreeIndex[T float32 | int32 | string](t *testing.T, keyType types.TypeID) {
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

	columnA := column.NewColumn("account_id", keyType, true, index_constants.INDEX_KIND_BTREE, types.PageID(-1), nil)
	columnB := column.NewColumn("balance", types.Integer, true, index_constants.INDEX_KIND_BTREE, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})
	tableMetadata := c.CreateTable("test_1", schema_, txn)

	txnMgr.Commit(c, txn)

	txn = txnMgr.Begin(nil)

	var accountId interface{}
	switch keyType {
	case types.Integer:
		accountId = int32(10)
	case types.Float:
		accountId = float32(-5.2)
	case types.Varchar:
		accountId = "duplicateTest"
	default:
		panic("unsuppoted value type")
	}

	insPlan1 := createSpecifiedValInsertPlanNode(accountId.(T), int32(100), c, tableMetadata, keyType)
	result := executePlan(c, shi.GetBufferPoolManager(), txn, insPlan1)
	insPlan2 := createSpecifiedValInsertPlanNode(accountId.(T), int32(101), c, tableMetadata, keyType)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, insPlan2)
	insPlan3 := createSpecifiedValInsertPlanNode(accountId.(T), int32(102), c, tableMetadata, keyType)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, insPlan3)

	txnMgr.Commit(c, txn)

	txn = txnMgr.Begin(nil)

	//rangeScanP := createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, nil, nil, index_constants.INDEX_KIND_BTREE)
	//results := executePlan(c, shi.GetBufferPoolManager(), txn, rangeScanP)
	//for _, foundVal := range results {
	//	fmt.Println(foundVal.GetValue(tableMetadata.Schema(), 0).ToString())
	//}

	scanP := createSpecifiedPointScanPlanNode(accountId.(T), c, tableMetadata, keyType, index_constants.INDEX_KIND_BTREE)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, scanP)
	testingpkg.Assert(t, len(result) == 3, "duplicated key point scan got illegal results.")
	rid1 := result[0].GetRID()
	val0_1 := result[0].GetValue(tableMetadata.Schema(), 0)
	val0_2 := result[0].GetValue(tableMetadata.Schema(), 1)
	fmt.Println(val0_1, val0_2)
	rid2 := result[1].GetRID()
	rid3 := result[2].GetRID()
	fmt.Printf("%v %v %v\n", *rid1, *rid2, *rid3)

	for _, foundTuple := range result {
		val := foundTuple.GetValue(tableMetadata.Schema(), 0)
		fmt.Println(val.ToString())
	}

	indexCol1 := tableMetadata.GetIndex(0)
	indexCol2 := tableMetadata.GetIndex(1)

	indexCol1.DeleteEntry(result[0], *rid1, txn)
	indexCol2.DeleteEntry(result[0], *rid1, txn)
	scanP = createSpecifiedPointScanPlanNode(accountId.(T), c, tableMetadata, keyType, index_constants.INDEX_KIND_BTREE)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, scanP)
	testingpkg.Assert(t, len(result) == 2, "duplicated key point scan got illegal results.")

	indexCol1.DeleteEntry(result[0], *rid2, txn)
	indexCol2.DeleteEntry(result[0], *rid2, txn)
	scanP = createSpecifiedPointScanPlanNode(accountId.(T), c, tableMetadata, keyType, index_constants.INDEX_KIND_BTREE)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, scanP)
	testingpkg.Assert(t, len(result) == 1, "duplicated key point scan got illegal results.")

	indexCol1.DeleteEntry(result[0], *rid3, txn)
	indexCol2.DeleteEntry(result[0], *rid3, txn)
	scanP = createSpecifiedPointScanPlanNode(accountId.(T), c, tableMetadata, keyType, index_constants.INDEX_KIND_BTREE)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, scanP)
	testingpkg.Assert(t, len(result) == 0, "duplicated key point scan got illegal results.")

	txnMgr.Commit(c, txn)
	shi.Shutdown(samehada.ShutdownPatternCloseFiles)
}

func TestKeyDuplicateInsertDeleteWithBTreeIndexInt(t *testing.T) {
	testKeyDuplicateInsertDeleteWithBTreeIndex[int32](t, types.Integer)
}

func TestKeyDuplicateInsertDeleteWithBTreeIndexFloat(t *testing.T) {
	testKeyDuplicateInsertDeleteWithBTreeIndex[float32](t, types.Float)
}

func TestKeyDuplicateInsertDeleteWithBTreeIndexVarchar(t *testing.T) {
	testKeyDuplicateInsertDeleteWithBTreeIndex[string](t, types.Varchar)
}

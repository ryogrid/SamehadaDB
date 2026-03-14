// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package catalog_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/samehada"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/column"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	testingpkg "github.com/ryogrid/SamehadaDB/lib/testing/testing_assert"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"os"
	"testing"
)

// test reloading serialized catalog info in db file at lauching system
func TestTableCatalogReload(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true

	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage {
		os.Remove(t.Name() + ".db")
	}
	samehadaInstance := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	bpm := buffer.NewBufferPoolManager(uint32(32), samehadaInstance.GetDiskManager(), samehadaInstance.GetLogManager())

	txn := samehadaInstance.GetTransactionManager().Begin(nil)
	catalogOld := catalog.BootstrapCatalog(bpm, samehadaInstance.GetLogManager(), samehadaInstance.GetLockManager(), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.IndexKindInvalid, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, true, index_constants.IndexKindHash, types.PageID(-1), nil)
	sc := schema.NewSchema([]*column.Column{columnA, columnB})

	catalogOld.CreateTable("test_1", sc, txn)
	bpm.FlushAllPages()

	samehadaInstance.CloseFilesForTesting()

	fmt.Println("Shutdown system...")

	samehadaInstanceNew := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	txnNew := samehadaInstanceNew.GetTransactionManager().Begin(nil)
	catalogRecov := catalog.RecoveryCatalogFromCatalogPage(samehadaInstanceNew.GetBufferPoolManager(), samehadaInstanceNew.GetLogManager(), samehadaInstanceNew.GetLockManager(), txnNew, true)

	columnToCheck := catalogRecov.GetTableByOID(1).Schema().GetColumn(1)

	testingpkg.Assert(t, columnToCheck.GetColumnName() == "test_1.b", "")
	testingpkg.Assert(t, columnToCheck.GetType() == 4, "")
	testingpkg.Assert(t, columnToCheck.HasIndex() == true, "")

	common.TempSuppressOnMemStorage = false
	common.TempSuppressOnMemStorageMutex.Unlock()
}

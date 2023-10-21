// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package catalog_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/samehada"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	testingpkg "github.com/ryogrid/SamehadaDB/testing/testing_assert"
	"github.com/ryogrid/SamehadaDB/types"
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
	samehada_instance := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	bpm := buffer.NewBufferPoolManager(uint32(32), samehada_instance.GetDiskManager(), samehada_instance.GetLogManager())

	txn := samehada_instance.GetTransactionManager().Begin(nil)
	catalog_old := catalog.BootstrapCatalog(bpm, samehada_instance.GetLogManager(), samehada_instance.GetLockManager(), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, true, index_constants.INDEX_KIND_HASH, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	catalog_old.CreateTable("test_1", schema_, txn)
	bpm.FlushAllPages()

	samehada_instance.CloseFilesForTesting()

	fmt.Println("Shutdown system...")

	samehada_instance_new := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	txn_new := samehada_instance_new.GetTransactionManager().Begin(nil)
	catalog_recov := catalog.RecoveryCatalogFromCatalogPage(samehada_instance_new.GetBufferPoolManager(), samehada_instance_new.GetLogManager(), samehada_instance_new.GetLockManager(), txn_new)

	columnToCheck := catalog_recov.GetTableByOID(1).Schema().GetColumn(1)

	testingpkg.Assert(t, columnToCheck.GetColumnName() == "test_1.b", "")
	testingpkg.Assert(t, columnToCheck.GetType() == 4, "")
	testingpkg.Assert(t, columnToCheck.HasIndex() == true, "")

	common.TempSuppressOnMemStorage = false
	common.TempSuppressOnMemStorageMutex.Unlock()
}

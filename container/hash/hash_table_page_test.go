// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package hash

import (
	"os"
	"testing"
	"unsafe"

	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/disk"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/types"
)

func TestHashTableHeaderPage(t *testing.T) {
	diskManager := disk.NewDiskManagerImpl("test.db")
	//bpm := buffer.NewBufferPoolManager(diskManager, buffer.NewClockReplacer(5))
	bpm := buffer.NewBufferPoolManager(10, diskManager)

	newPage := bpm.NewPage()
	newPageData := newPage.Data()

	headerPage := (*page.HashTableHeaderPage)(unsafe.Pointer(newPageData))

	for i := 0; i < 11; i++ {
		headerPage.SetSize(i)
		if i != headerPage.GetSize() {
			t.Errorf("GetSize shoud be %d, but got %d", i, headerPage.GetSize())
		}

		//headerPage.SetPageId(page.PageID(i))
		headerPage.SetPageId(types.PageID(i))
		if types.PageID(i) != headerPage.GetPageId() {
			t.Errorf("GetPageId shoud be %d, but got %d", types.PageID(i), headerPage.GetPageId())
		}

		headerPage.SetLSN(i)
		if i != headerPage.GetLSN() {
			t.Errorf("GetLSN shoud be %d, but got %d", i, headerPage.GetLSN())
		}
	}

	// add a few hypothetical block pages
	for i := 0; i < 10; i++ {
		headerPage.AddBlockPageId(types.PageID(i))
		if i+1 != headerPage.NumBlocks() {
			t.Errorf("NumBlocks shoud be %d, but got %d", i+1, headerPage.NumBlocks())
		}
	}

	// check for correct block page IDs
	for i := 0; i < 10; i++ {
		if types.PageID(i) != headerPage.GetBlockPageId(i) {
			t.Errorf("GetBlockPageId shoud be %d, but got %d", i, headerPage.GetBlockPageId(i))
		}
	}

	// unpin the header page now that we are done
	bpm.UnpinPage(headerPage.GetPageId(), true)
	diskManager.ShutDown()
	os.Remove("test.db")
}

// func TestHashTableBlockPage(t *testing.T) {
// 	diskManager := disk.NewDiskManagerImpl("test.db")
// 	bpm := buffer.NewBufferPoolManager(diskManager, buffer.NewClockReplacer(5))

// 	newPage := bpm.NewPage()
// 	newPageData := newPage.Data()

// 	blockPage := (*page.HashTableBlockPage)(unsafe.Pointer(newPageData))

// 	for i := 0; i < 10; i++ {
// 		blockPage.Insert(i, i, i)
// 	}

// 	for i := 0; i < 10; i++ {
// 		equals(t, i, blockPage.KeyAt(i))
// 		equals(t, i, blockPage.ValueAt(i))
// 	}

// 	for i := 0; i < 10; i++ {
// 		if i%2 == 1 {
// 			blockPage.Remove(i)
// 		}
// 	}

// 	for i := 0; i < 15; i++ {
// 		if i < 10 {
// 			assert(t, true == blockPage.IsOccupied(i), "block page should be occupied")
// 			if i%2 == 1 {
// 				assert(t, false == blockPage.IsReadable(i), "block page should not be readable")
// 			} else {
// 				assert(t, true == blockPage.IsReadable(i), "block page should be readable")
// 			}
// 		} else {
// 			assert(t, false == blockPage.IsOccupied(i), "block page should not be occupied")
// 		}
// 	}

// 	bpm.UnpinPage(newPage.ID(), true)
// 	bpm.FlushAllpages()
// 	os.Remove("test.db")
// }

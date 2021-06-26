package hash

import (
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

// import (
// 	"fmt"
// 	"os"
// 	"path/filepath"
// 	"reflect"
// 	"runtime"
// 	"testing"
// 	"unsafe"

// 	"github.com/brunocalza/go-bustub/buffer"
// 	"github.com/brunocalza/go-bustub/storage/disk"
// 	"github.com/brunocalza/go-bustub/storage/page"
// )

// func TestHashTableHeaderPage(t *testing.T) {
// 	diskManager := disk.NewDiskManagerImpl("test.db")
// 	bpm := buffer.NewBufferPoolManager(diskManager, buffer.NewClockReplacer(5))

// 	newPage := bpm.NewPage()
// 	newPageData := newPage.Data()

// 	headerPage := (*page.HashTableHeaderPage)(unsafe.Pointer(newPageData))

// 	for i := 0; i < 11; i++ {
// 		headerPage.SetSize(i)
// 		if i != headerPage.GetSize() {
// 			t.Errorf("GetSize shoud be %d, but got %d", i, headerPage.GetSize())
// 		}

// 		headerPage.SetPageId(page.PageID(i))
// 		if page.PageID(i) != headerPage.GetPageId() {
// 			t.Errorf("GetPageId shoud be %d, but got %d", page.PageID(i), headerPage.GetPageId())
// 		}

// 		headerPage.SetLSN(i)
// 		if i != headerPage.GetLSN() {
// 			t.Errorf("GetLSN shoud be %d, but got %d", i, headerPage.GetLSN())
// 		}
// 	}

// 	// add a few hypothetical block pages
// 	for i := 0; i < 10; i++ {
// 		headerPage.AddBlockPageId(page.PageID(i))
// 		if i+1 != headerPage.NumBlocks() {
// 			t.Errorf("NumBlocks shoud be %d, but got %d", i+1, headerPage.NumBlocks())
// 		}
// 	}

// 	// check for correct block page IDs
// 	for i := 0; i < 10; i++ {
// 		if page.PageID(i) != headerPage.GetBlockPageId(i) {
// 			t.Errorf("GetBlockPageId shoud be %d, but got %d", i, headerPage.GetBlockPageId(i))
// 		}
// 	}

// 	// unpin the header page now that we are done
// 	bpm.UnpinPage(headerPage.GetPageId(), true)
// 	diskManager.ShutDown()
// 	os.Remove("test.db")
// }

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

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// nok fails the test if an err is nil.
func nok(tb testing.TB, err error) {
	if err == nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: err should not be nil\n\n", filepath.Base(file), line)
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}

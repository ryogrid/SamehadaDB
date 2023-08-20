// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package disk

import (
	testingpkg "github.com/ryogrid/SamehadaDB/testing/testing_assert"
	"testing"

	"github.com/ryogrid/SamehadaDB/common"
)

func zeroClear(buffer []byte) {
	for i := range buffer {
		buffer[i] = 0
	}
}

func TestReadWritePage(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true

	dm := NewDiskManagerTest()
	defer dm.ShutDown()

	data := make([]byte, common.PageSize)
	buffer := make([]byte, common.PageSize)

	copy(data, "A test string.")

	dm.ReadPage(0, buffer) // tolerate empty read
	dm.WritePage(0, data)
	err := dm.ReadPage(0, buffer)
	testingpkg.Equals(t, err, nil)
	testingpkg.Equals(t, int64(common.PageSize), dm.Size())
	testingpkg.Equals(t, data, buffer)

	zeroClear(buffer)
	copy(data, "Another test string.")

	dm.WritePage(5, data)
	dm.ReadPage(5, buffer)
	testingpkg.Equals(t, data, buffer)

	// the size of disk is 6 * PageSize bytes because we have 6 pages
	testingpkg.Equals(t, int64(6*common.PageSize), dm.Size())

	common.TempSuppressOnMemStorage = false
	common.TempSuppressOnMemStorageMutex.Unlock()
}

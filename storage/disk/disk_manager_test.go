package disk

import (
	"testing"

	"github.com/brunocalza/go-bustub/storage/page"
	"github.com/brunocalza/go-bustub/testingutils"
)

func TestReadWritePage(t *testing.T) {
	dm := NewDiskManagerTest()
	defer dm.ShutDown()

	data := make([]byte, page.PageSize)
	buffer := make([]byte, page.PageSize)

	copy(data, "A test string.")

	dm.ReadPage(0, buffer) // tolerate empty read
	dm.WritePage(0, data)
	dm.ReadPage(0, buffer)
	testingutils.Equals(t, data, buffer)

	memset(buffer, 0)
	copy(data, "Another test string.")

	dm.WritePage(5, data)
	dm.ReadPage(5, buffer)
	testingutils.Equals(t, data, buffer)
}

func memset(buffer []byte, value int) {
	for i := range buffer {
		buffer[i] = 0
	}
}

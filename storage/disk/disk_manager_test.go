package disk

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/brunocalza/go-bustub/storage/page"
	"github.com/brunocalza/go-bustub/testingutils"
)

//DiskManagerTest is the disk implementation of DiskManager for testing purposes
type DiskManagerTest struct {
	path string
	DiskManager
}

// NewDiskManagerTest returns a DiskManager instance for testing purposes
func NewDiskManagerTest() DiskManager {
	// Retrieve a temporary path.
	f, err := ioutil.TempFile("", "")
	if err != nil {
		panic(err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)

	diskManager := NewDiskManagerImpl(path)
	return &DiskManagerTest{path, diskManager}
}

// ShutDown closes of the database file
func (d *DiskManagerTest) ShutDown() {
	defer os.Remove(d.path)
	d.DiskManager.ShutDown()
}

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

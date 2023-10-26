// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package disk

import (
	"github.com/ryogrid/SamehadaDB/lib/common"
	"io/ioutil"
	"os"
)

// DiskManagerTest is the disk implementation of DiskManager for testing purposes
type DiskManagerTest struct {
	path string
	DiskManager
}

// NewDiskManagerTest returns a DiskManager instance for testing purposes
func NewDiskManagerTest() DiskManager {
	// Retrieve a temporary path.
	f, err := ioutil.TempFile("", "samehada.")
	if err != nil {
		panic(err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)

	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage {
		diskManager := NewDiskManagerImpl(path)
		return &DiskManagerTest{path, diskManager}
	} else {
		diskManager := NewVirtualDiskManagerImpl(path)
		return &DiskManagerTest{path, diskManager}
	}
}

// ShutDown closes of the database file
func (d *DiskManagerTest) ShutDown() {
	d.DiskManager.ShutDown()
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage {
		os.Remove(d.path)
	}
}

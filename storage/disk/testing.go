// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package disk

import (
	"io/ioutil"
	"os"
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

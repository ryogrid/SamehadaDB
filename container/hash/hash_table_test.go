package hash

import (
	"testing"

	"github.com/ryogrid/SaitomDB/storage/buffer"
	"github.com/ryogrid/SaitomDB/storage/disk"
	testingpkg "github.com/ryogrid/SaitomDB/testing"
)

func TestHashTable(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	bpm := buffer.NewBufferPoolManager(10, diskManager)

	ht := NewHashTable(bpm, 1000)

	for i := 0; i < 5; i++ {
		ht.Insert(i, i)
		res := ht.GetValue(i)
		if len(res) == 0 {
			t.Errorf("result should not be nil")
		} else {
			testingpkg.Equals(t, i, res[0])
		}
	}

	for i := 0; i < 5; i++ {
		res := ht.GetValue(i)
		if len(res) == 0 {
			t.Errorf("result should not be nil")
		} else {
			testingpkg.Equals(t, i, res[0])
		}
	}

	// test for duplicate values
	for i := 0; i < 5; i++ {
		if i == 0 {
			testingpkg.Nok(t, ht.Insert(i, 2*i))
		} else {
			testingpkg.Ok(t, ht.Insert(i, 2*i))
		}
		ht.Insert(i, 2*i)
		res := ht.GetValue(i)
		if i == 0 {
			testingpkg.Equals(t, 1, len(res))
			testingpkg.Equals(t, i, res[0])
		} else {
			testingpkg.Equals(t, 2, len(res))
			if res[0] == i {
				testingpkg.Equals(t, 2*i, res[1])
			} else {
				testingpkg.Equals(t, 2*i, res[0])
				testingpkg.Equals(t, i, res[1])
			}
		}
	}

	// look for a key that does not exist
	res := ht.GetValue(20)
	testingpkg.Equals(t, 0, len(res))

	// delete some values
	for i := 0; i < 5; i++ {
		ht.Remove(i, i)
		res := ht.GetValue(i)

		if i == 0 {
			testingpkg.Equals(t, 0, len(res))
		} else {
			testingpkg.Equals(t, 1, len(res))
			testingpkg.Equals(t, 2*i, res[0])
		}
	}

	bpm.FlushAllpages()
}

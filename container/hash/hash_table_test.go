package hash

import (
	"testing"

	"github.com/brunocalza/go-bustub/storage/buffer"
	"github.com/brunocalza/go-bustub/storage/disk"
	"github.com/brunocalza/go-bustub/testingutils"
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
			testingutils.Equals(t, i, res[0])
		}
	}

	for i := 0; i < 5; i++ {
		res := ht.GetValue(i)
		if len(res) == 0 {
			t.Errorf("result should not be nil")
		} else {
			testingutils.Equals(t, i, res[0])
		}
	}

	// test for duplicate values
	for i := 0; i < 5; i++ {
		if i == 0 {
			testingutils.Nok(t, ht.Insert(i, 2*i))
		} else {
			testingutils.Ok(t, ht.Insert(i, 2*i))
		}
		ht.Insert(i, 2*i)
		res := ht.GetValue(i)
		if i == 0 {
			testingutils.Equals(t, 1, len(res))
			testingutils.Equals(t, i, res[0])
		} else {
			testingutils.Equals(t, 2, len(res))
			if res[0] == i {
				testingutils.Equals(t, 2*i, res[1])
			} else {
				testingutils.Equals(t, 2*i, res[0])
				testingutils.Equals(t, i, res[1])
			}
		}
	}

	// look for a key that does not exist
	res := ht.GetValue(20)
	testingutils.Equals(t, 0, len(res))

	// delete some values
	for i := 0; i < 5; i++ {
		ht.Remove(i, i)
		res := ht.GetValue(i)

		if i == 0 {
			testingutils.Equals(t, 0, len(res))
		} else {
			testingutils.Equals(t, 1, len(res))
			testingutils.Equals(t, 2*i, res[0])
		}
	}

	bpm.FlushAllpages()
}

package hash

import (
	"testing"

	"github.com/brunocalza/go-bustub/buffer"
	"github.com/brunocalza/go-bustub/storage/disk"
)

func TestHashTable(t *testing.T) {
	diskManager := disk.NewDiskManagerImpl("test.db")
	bpm := buffer.NewBufferPoolManager(diskManager, buffer.NewClockReplacer(10))

	ht := NewHashTable(bpm, 1000)

	for i := 0; i < 5; i++ {
		ht.Insert(i, i)
		res := ht.GetValue(i)
		if len(res) == 0 {
			t.Errorf("result should not be nil")
		} else {
			equals(t, i, res[0])
		}
	}

	for i := 0; i < 5; i++ {
		res := ht.GetValue(i)
		if len(res) == 0 {
			t.Errorf("result should not be nil")
		} else {
			equals(t, i, res[0])
		}
	}

	// test for duplicate values
	for i := 0; i < 5; i++ {
		if i == 0 {
			nok(t, ht.Insert(i, 2*i))
		} else {
			ok(t, ht.Insert(i, 2*i))
		}
		ht.Insert(i, 2*i)
		res := ht.GetValue(i)
		if i == 0 {
			equals(t, 1, len(res))
			equals(t, i, res[0])
		} else {
			equals(t, 2, len(res))
			if res[0] == i {
				equals(t, 2*i, res[1])
			} else {
				equals(t, 2*i, res[0])
				equals(t, i, res[1])
			}
		}
	}

	// look for a key that does not exist
	res := ht.GetValue(20)
	equals(t, 0, len(res))

	// delete some values
	for i := 0; i < 5; i++ {
		ht.Remove(i, i)
		res := ht.GetValue(i)

		if i == 0 {
			equals(t, 0, len(res))
		} else {
			equals(t, 1, len(res))
			equals(t, 2*i, res[0])
		}
	}

	bpm.FlushAllpages()
}

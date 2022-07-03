// TODO: (SDB) not implemented yet skip_list_test.go

package skip_list

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/disk"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
)

func IntToBytes(val int) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(val))
	return buf.Bytes()
}

func TestSkipList(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	bpm := buffer.NewBufferPoolManager(uint32(10), diskManager, recovery.NewLogManager(&diskManager))

	sl := NewSkipList(bpm, 1000)

	for i := 0; i < 5; i++ {
		sl.Insert(IntToBytes(i), uint32(i))
		res := sl.GetValue(IntToBytes(i))
		if len(res) == 0 {
			t.Errorf("result should not be nil")
		} else {
			testingpkg.Equals(t, uint32(i), res[0])
		}
	}

	for i := 0; i < 5; i++ {
		res := sl.GetValue(IntToBytes(i))
		if len(res) == 0 {
			t.Errorf("result should not be nil")
		} else {
			testingpkg.Equals(t, uint32(i), res[0])
		}
	}

	// test for duplicate values
	for i := 0; i < 5; i++ {
		if i == 0 {
			testingpkg.Nok(t, sl.Insert(IntToBytes(i), uint32(2*i)))
		} else {
			testingpkg.Ok(t, sl.Insert(IntToBytes(i), uint32(2*i)))
		}
		sl.Insert(IntToBytes(i), uint32(2*i))
		res := sl.GetValue(IntToBytes(i))
		if i == 0 {
			testingpkg.Equals(t, 1, len(res))
			testingpkg.Equals(t, uint32(i), res[0])
		} else {
			testingpkg.Equals(t, 2, len(res))
			if res[0] == uint32(i) {
				testingpkg.Equals(t, uint32(2*i), res[1])
			} else {
				testingpkg.Equals(t, uint32(2*i), res[0])
				testingpkg.Equals(t, uint32(i), res[1])
			}
		}
	}

	// look for a key that does not exist
	res := sl.GetValue(IntToBytes(20))
	testingpkg.Equals(t, 0, len(res))

	// delete some values
	for i := 0; i < 5; i++ {
		sl.Remove(IntToBytes(i), uint32(i))
		res := sl.GetValue(IntToBytes(i))

		if i == 0 {
			testingpkg.Equals(t, 0, len(res))
		} else {
			testingpkg.Equals(t, 1, len(res))
			testingpkg.Equals(t, uint32(2*i), res[0])
		}
	}

	bpm.FlushAllPages()
}

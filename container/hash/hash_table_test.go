// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package hash

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/disk"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
)

func IntToBytes(val int) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func TestLinearProbeHashTable(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	bpm := buffer.NewBufferPoolManager(uint32(10), diskManager)

	ht := NewLinearProbeHashTable(bpm, 1000)

	for i := 0; i < 5; i++ {
		ht.Insert(IntToBytes(i), uint32(i))
		res := ht.GetValue(IntToBytes(i))
		if len(res) == 0 {
			t.Errorf("result should not be nil")
		} else {
			testingpkg.Equals(t, uint32(i), res[0])
		}
	}

	for i := 0; i < 5; i++ {
		res := ht.GetValue(IntToBytes(i))
		if len(res) == 0 {
			t.Errorf("result should not be nil")
		} else {
			testingpkg.Equals(t, uint32(i), res[0])
		}
	}

	// test for duplicate values
	for i := 0; i < 5; i++ {
		if i == 0 {
			testingpkg.Nok(t, ht.Insert(IntToBytes(i), uint32(2*i)))
		} else {
			testingpkg.Ok(t, ht.Insert(IntToBytes(i), uint32(2*i)))
		}
		ht.Insert(IntToBytes(i), uint32(2*i))
		res := ht.GetValue(IntToBytes(i))
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
	res := ht.GetValue(IntToBytes(20))
	testingpkg.Equals(t, 0, len(res))

	// delete some values
	for i := 0; i < 5; i++ {
		ht.Remove(IntToBytes(i), uint32(i))
		res := ht.GetValue(IntToBytes(i))

		if i == 0 {
			testingpkg.Equals(t, 0, len(res))
		} else {
			testingpkg.Equals(t, 1, len(res))
			testingpkg.Equals(t, uint32(2*i), res[0])
		}
	}

	bpm.FlushAllPages()
}

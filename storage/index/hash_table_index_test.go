package index

import (
	"fmt"
	"testing"

	"github.com/ryogrid/SamehadaDB/storage/page"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
)

func TestHashTableIndex(t *testing.T) {
	testingpkg.Assert(t, false, "TestHashTableIndex is not implemented yet")
}

func TestPackAndUnpackRID(t *testing.T) {
	rid := new(page.RID)
	rid.PageId = 55
	rid.SlotNum = 1027

	packed_val := PackRIDtoUint32(rid)
	fmt.Println(packed_val)
	unpacked_val := UnpackUint32toRID(packed_val)

	testingpkg.Assert(t, unpacked_val.PageId == 55, "")
	testingpkg.Assert(t, unpacked_val.SlotNum == 1027, "")
}

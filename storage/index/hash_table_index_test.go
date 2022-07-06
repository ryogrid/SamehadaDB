package index

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"testing"

	"github.com/ryogrid/SamehadaDB/storage/page"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
)

func TestPackAndUnpackRID(t *testing.T) {
	rid := new(page.RID)
	rid.PageId = 55
	rid.SlotNum = 1027

	packed_val := samehada_util.PackRIDtoUint32(rid)
	fmt.Println(packed_val)
	unpacked_val := samehada_util.UnpackUint32toRID(packed_val)

	testingpkg.Assert(t, unpacked_val.PageId == 55, "")
	testingpkg.Assert(t, unpacked_val.SlotNum == 1027, "")
}

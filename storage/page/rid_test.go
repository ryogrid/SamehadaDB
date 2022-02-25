// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package page

import (
	"testing"

	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
)

func TestRID(t *testing.T) {
	rid := RID{}
	rid.Set(types.PageID(0), uint32(0))
	testingpkg.Equals(t, types.PageID(0), rid.GetPageId())
	testingpkg.Equals(t, uint32(0), rid.GetSlotNum())
}

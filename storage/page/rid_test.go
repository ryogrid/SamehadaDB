package page

import (
	"testing"

	testingpkg "github.com/brunocalza/go-bustub/testing"
	"github.com/brunocalza/go-bustub/types"
)

func TestRID(t *testing.T) {
	rid := RID{}
	rid.Set(types.PageID(0), uint32(0))
	testingpkg.Equals(t, types.PageID(0), rid.GetPageId())
	testingpkg.Equals(t, uint32(0), rid.GetSlot())
}

package page

import (
	"testing"

	testingpkg "github.com/brunocalza/go-bustub/testing"
)

func TestRID(t *testing.T) {
	rid := RID{}
	rid.Set(PageID(0), uint32(0))
	testingpkg.Equals(t, PageID(0), rid.GetPageId())
	testingpkg.Equals(t, uint32(0), rid.GetSlot())
}

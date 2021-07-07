package page

import (
	"testing"

	"github.com/brunocalza/go-bustub/testingutils"
)

func TestRID(t *testing.T) {
	rid := RID{}
	rid.Set(PageID(0), uint32(0))
	testingutils.Equals(t, PageID(0), rid.GetPageId())
	testingutils.Equals(t, uint32(0), rid.GetSlot())
}

package page

import (
	"testing"

	"github.com/brunocalza/go-bustub/testingutils"
)

func TestNewPage(t *testing.T) {
	p := New(PageID(0), false, &[PageSize]byte{})

	testingutils.Equals(t, PageID(0), p.ID())
	testingutils.Equals(t, uint32(1), p.PinCount())
	p.IncPinCount()
	testingutils.Equals(t, uint32(2), p.PinCount())
	p.DecPinCount()
	p.DecPinCount()
	p.DecPinCount()
	testingutils.Equals(t, uint32(0), p.PinCount())
	testingutils.Equals(t, false, p.IsDirty())
	p.SetIsDirty(true)
	testingutils.Equals(t, true, p.IsDirty())
	p.CopyToData([]byte{'H', 'E', 'L', 'L', 'O'})
	testingutils.Equals(t, [PageSize]byte{'H', 'E', 'L', 'L', 'O'}, *p.Data())
}

func TestEmptyPage(t *testing.T) {
	p := NewEmpty(PageID(0))

	testingutils.Equals(t, PageID(0), p.ID())
	testingutils.Equals(t, uint32(1), p.PinCount())
	testingutils.Equals(t, false, p.IsDirty())
	testingutils.Equals(t, [PageSize]byte{}, *p.Data())
}

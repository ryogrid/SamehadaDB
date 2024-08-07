// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package page

import (
	testingpkg "github.com/ryogrid/SamehadaDB/lib/testing/testing_assert"
	"testing"

	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

func TestNewPage(t *testing.T) {
	p := New(types.PageID(0), false, &[common.PageSize]byte{})

	testingpkg.Equals(t, types.PageID(0), p.GetPageId())
	testingpkg.Equals(t, int32(1), p.PinCount())
	p.IncPinCount()
	testingpkg.Equals(t, int32(2), p.PinCount())
	p.DecPinCount()
	p.DecPinCount()
	testingpkg.Equals(t, int32(0), p.PinCount())
	testingpkg.Equals(t, false, p.IsDirty())
	p.SetIsDirty(true)
	testingpkg.Equals(t, true, p.IsDirty())
	p.Copy(0, []byte{'H', 'E', 'L', 'L', 'O'})
	testingpkg.Equals(t, [common.PageSize]byte{'H', 'E', 'L', 'L', 'O'}, *p.Data())
}

func TestEmptyPage(t *testing.T) {
	var pageData [common.PageSize]byte
	p := NewEmpty(types.PageID(0), &pageData)

	testingpkg.Equals(t, types.PageID(0), p.GetPageId())
	testingpkg.Equals(t, int32(1), p.PinCount())
	testingpkg.Equals(t, false, p.IsDirty())
	testingpkg.Equals(t, [common.PageSize]byte{}, *p.Data())
}

// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package page

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/types"
)

// // PageSize is the size of a page in disk (4KB)
// const PageSize = 4096

// Page represents an abstract page on disk
type Page struct {
	id       types.PageID           // idenfies the page. It is used to find the offset of the page on disk
	pinCount uint32                 // counts how many goroutines are acessing it
	isDirty  bool                   // the page was modified but not flushed
	data     *[common.PageSize]byte // bytes stored in disk
	// TODO: (SDB) need to use latch at Page
}

// IncPinCount decrements pin count
func (p *Page) IncPinCount() {
	p.pinCount++
}

// DecPinCount decrements pin count
func (p *Page) DecPinCount() {
	if p.pinCount > 0 {
		p.pinCount--
	}
}

// PinCount retunds the pin count
func (p *Page) PinCount() uint32 {
	return p.pinCount
}

// ID retunds the page id
func (p *Page) ID() types.PageID {
	return p.id
}

// Data returns the data of the page
func (p *Page) Data() *[common.PageSize]byte {
	return p.data
}

// SetIsDirty sets the isDirty bit
func (p *Page) SetIsDirty(isDirty bool) {
	p.isDirty = isDirty
}

// IsDirty check if the page is dirty
func (p *Page) IsDirty() bool {
	return p.isDirty
}

// Copy copies data to the page's data. It is mainly used for testing
func (p *Page) Copy(offset uint32, data []byte) {
	copy(p.data[offset:], data)
}

// New creates a new page
func New(id types.PageID, isDirty bool, data *[common.PageSize]byte) *Page {
	return &Page{id, uint32(1), isDirty, data}
}

// New creates a new empty page
func NewEmpty(id types.PageID) *Page {
	return &Page{id, uint32(1), false, &[common.PageSize]byte{}}
}

// TODO: (SDB) lockking and unlockking latch of Page is not implemented yet
//   /** Acquire the page write latch. */
//   inline void WLatch() { rwlatch_.WLock(); }

//   /** Release the page write latch. */
//   inline void WUnlatch() { rwlatch_.WUnlock(); }

//   /** Acquire the page read latch. */
//   inline void RLatch() { rwlatch_.RLock(); }

//   /** Release the page read latch. */
//   inline void RUnlatch() { rwlatch_.RUnlock(); }

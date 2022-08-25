// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package page

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/types"
)

// PageSize is the size of a page in disk (4KB)
//const PageSize = 4096
const SizePageHeader = 8
const OffsetPageStart = 0
const OffsetLSN = 4

/**
 * Page is the basic unit of storage within the database system. Page provides a wrapper for actual data pages being
 * held in main memory. Page also contains book-keeping information that is used by the buffer pool manager, e.g.
 * pin count, dirty flag, page id, etc.
 */

// Page represents an abstract page on disk
type Page struct {
	id       types.PageID           // idenfies the page. It is used to find the offset of the page on disk
	pinCount uint32                 // counts how many goroutines are acessing it
	isDirty  bool                   // the page was modified but not flushed
	data     *[common.PageSize]byte // bytes stored in disk
	rwlatch_ common.ReaderWriterLatch
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

// Copy copies data to the page's data
func (p *Page) Copy(offset uint32, data []byte) {
	copy(p.data[offset:], data)
}

// New creates a new page
func New(id types.PageID, isDirty bool, data *[common.PageSize]byte) *Page {
	return &Page{id, uint32(1), isDirty, data, common.NewRWLatch()}
}

// New creates a new empty page
func NewEmpty(id types.PageID) *Page {
	return &Page{id, uint32(1), false, &[common.PageSize]byte{}, common.NewRWLatch()}
}

/** @return the page LSN. */
func (p *Page) GetLSN() types.LSN {
	/* return -1 */
	/**reinterpret_cast<lsn_t *>(GetData() + OFFSET_LSN)*/
	return types.NewLSNFromBytes(p.GetData()[OffsetLSN : OffsetLSN+types.SizeOfLSN])
}

/** Sets the page LSN. */
func (p *Page) SetLSN(lsn types.LSN) {
	/*memcpy(GetData() + OFFSET_LSN, &lsn, sizeof(lsn_t))*/
	copy(p.data[OffsetLSN:OffsetLSN+types.SizeOfLSN], lsn.Serialize())
}

func (p *Page) GetPageId() types.PageID { return p.id }

func (p *Page) GetData() *[common.PageSize]byte {
	return p.data
}

/** Acquire the page write latch. */
func (p *Page) WLatch() {
	// common.SH_Assert(!p.rwlatch_.IsWriteLocked(), "Page is already write locked")
	// fmt.Printf("Page::WLatch: page address %p\n", p)
	p.rwlatch_.WLock()
}

/** Release the page write latch. */
func (p *Page) WUnlatch() {
	// fmt.Printf("Page::WUnlatch: page address %p\n", p)
	p.rwlatch_.WUnlock()
}

/** Acquire the page read latch. */
func (p *Page) RLatch() {
	//common.SH_Assert(!p.rwlatch_.IsReadLocked(), "Page is already read locked")
	// fmt.Printf("Page::RLatch: page address %p\n", p)
	p.rwlatch_.RLock()
}

/** Release the page read latch. */
func (p *Page) RUnlatch() {
	// fmt.Printf("Page::RUnlatch: page address %p\n", p)
	p.rwlatch_.RUnlock()
}

func (p *Page) GetRWLachObj() common.ReaderWriterLatch {
	return p.rwlatch_
}

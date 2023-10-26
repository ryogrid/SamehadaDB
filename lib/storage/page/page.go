// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package page

import (
	//"github.com/sasha-s/go-deadlock"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"sync"
	"sync/atomic"
)

// PageSize is the size of a page in disk (4KB)
// const PageSize = 4096
const SizePageHeader = 8
const OffsetPageStart = 0
const OffsetLSN = 4

/**
 * Page is the basic unit of storage within the database system. Page provides a wrapper for actual data pages being
 * held in main memory. Page also contains book-keeping information that is used by the buffer pool manager, e.g.
 * pin count, dirty flag, page id, etc.
 */

// currently used for SkipList only
type PageIF interface {
	DecPinCount()
	IncPinCount()
}

// Page represents an abstract page on disk
type Page struct {
	id            types.PageID           // idenfies the page. It is used to find the offset of the page on disk
	pinCount      int32                  //int32                 // counts how many goroutines are acessing it
	isDirty       bool                   // the page was modified but not flushed
	isDeallocated bool                   // whether this is deallocated or not
	data          *[common.PageSize]byte // bytes stored in disk
	rwlatch_      common.ReaderWriterLatch
	// for debug
	WLatchMap      map[int32]bool
	RLatchMap      map[int32]bool
	RLatchMapMutex *sync.Mutex
}

// IncPinCount increments pin count
func (p *Page) IncPinCount() {
	atomic.AddInt32(&p.pinCount, 1)

	//common.ShPrintf(common.DEBUG_INFO, "pinCount of page-%d at IncPinCount: %d\n", p.GetPageId(), p.pinCount)
}

// DecPinCount decrements pin count
func (p *Page) DecPinCount() {
	//if p.pinCount > 0 {
	//common.SH_Assert(atomic.LoadInt32(&p.pinCount)-1 >= 0, fmt.Sprintf("pinCount becomes minus value! pageID:%d", p.GetPageId()))
	atomic.AddInt32(&p.pinCount, -1)
	//}

	//common.ShPrintf(common.DEBUG_INFO, "pinCount of page-%d at DecPinCount: %d\n", p.GetPageId(), p.pinCount)
}

// PinCount retunds the pin count
func (p *Page) PinCount() int32 {
	return atomic.LoadInt32(&p.pinCount)
}

// GetPageId retunds the page id
func (p *Page) GetPageId() types.PageID {
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

func (p *Page) IsDeallocated() bool {
	return p.isDeallocated
}

func (p *Page) SetIsDeallocated(isDeallocated bool) {
	p.isDeallocated = isDeallocated
}

// Copy copies data to the page's data
func (p *Page) Copy(offset uint32, data []byte) {
	copy(p.data[offset:], data)
}

// New creates a new page
func New(id types.PageID, isDirty bool, data *[common.PageSize]byte) *Page {
	return &Page{id, int32(1), isDirty, false, data, common.NewRWLatch(), make(map[int32]bool, 0), make(map[int32]bool, 0), new(sync.Mutex)}
}

// New creates a new empty page
func NewEmpty(id types.PageID) *Page {
	return &Page{id, int32(1), false, false, &[common.PageSize]byte{}, common.NewRWLatch(), make(map[int32]bool, 0), make(map[int32]bool, 0), new(sync.Mutex)}
}

/** @return the page LSN. */
func (p *Page) GetLSN() types.LSN {
	return types.NewLSNFromBytes(p.GetData()[OffsetLSN : OffsetLSN+types.SizeOfLSN])
}

/** Sets the page LSN. */
func (p *Page) SetLSN(lsn types.LSN) {
	copy(p.data[OffsetLSN:OffsetLSN+types.SizeOfLSN], lsn.Serialize())
}

func (p *Page) GetData() *[common.PageSize]byte {
	return p.data
}

/** Acquire the page write latch. */
func (p *Page) WLatch() {
	// fmt.Printf("Page::WLatch: page address %p\n", p)
	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO_DETAIL, "pageId=%d ", p.GetPageId())
	}

	p.rwlatch_.WLock()
}

/** Release the page write latch. */
func (p *Page) WUnlatch() {
	// fmt.Printf("Page::WUnlatch: page address %p\n", p)
	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO_DETAIL, "pageId=%d ", p.GetPageId())
	}
	p.rwlatch_.WUnlock()
}

/** Acquire the page read latch. */
func (p *Page) RLatch() {
	// fmt.Printf("Page::RLatch: page address %p\n", p)
	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO_DETAIL, "pageId=%d ", p.GetPageId())
	}
	p.rwlatch_.RLock()
}

/** Release the page read latch. */
func (p *Page) RUnlatch() {
	// fmt.Printf("Page::RUnlatch: page address %p\n", p)
	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO_DETAIL, "pageId=%d ", p.GetPageId())
	}
	p.rwlatch_.RUnlock()
}

func (p *Page) PrintMutexDebugInfo() {
	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO_DETAIL, "pageId=%d ", p.GetPageId())
	}
	p.rwlatch_.PrintDebugInfo()
}

func (p *Page) PrintPinCount() {
	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO, "PageId=%d PinCount=%d\n", p.id, p.PinCount())
	}
}

func (p *Page) GetRWLachObj() common.ReaderWriterLatch {
	return p.rwlatch_
}

// for debugging
func (p *Page) AddWLatchRecord(info int32) {
	//p.WLatchMap[info] = true
}
func (p *Page) RemoveWLatchRecord(info int32) {
	//delete(p.WLatchMap, info)
}
func (p *Page) AddRLatchRecord(info int32) {
	//p.RLatchMapMutex.Lock()
	//p.RLatchMap[info] = true
	//p.RLatchMapMutex.Unlock()
}
func (p *Page) RemoveRLatchRecord(info int32) {
	//p.RLatchMapMutex.Lock()
	//delete(p.RLatchMap, info)
	//p.RLatchMapMutex.Unlock()
}

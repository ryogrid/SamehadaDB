// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package buffer

import (
	"errors"
	"sync"

	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/disk"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/types"
)

// BufferPoolManager represents the buffer pool manager
type BufferPoolManager struct {
	diskManager disk.DiskManager
	pages       []*page.Page
	replacer    *ClockReplacer
	freeList    []FrameID
	pageTable   map[types.PageID]FrameID
	log_manager *recovery.LogManager
	mutex       *sync.Mutex
}

// FetchPage fetches the requested page from the buffer pool.
func (b *BufferPoolManager) FetchPage(pageID types.PageID) *page.Page {
	// if it is on buffer pool return it
	b.mutex.Lock()
	if frameID, ok := b.pageTable[pageID]; ok {
		pg := b.pages[frameID]
		pg.IncPinCount()
		(*b.replacer).Pin(frameID)
		b.mutex.Unlock()
		return pg
	}
	b.mutex.Unlock()

	// get the id from free list or from replacer
	frameID, isFromFreeList := b.getFrameID()
	if frameID == nil {
		return nil
	}

	if !isFromFreeList {
		// remove page from current frame
		b.mutex.Lock()
		currentPage := b.pages[*frameID]
		b.mutex.Unlock()
		if currentPage != nil {
			if currentPage.IsDirty() {
				b.log_manager.Flush()
				currentPage.WLatch()
				data := currentPage.Data()
				b.diskManager.WritePage(currentPage.ID(), data[:])
				currentPage.WUnlatch()
			}
			b.mutex.Lock()
			delete(b.pageTable, currentPage.ID())
			b.mutex.Unlock()
		}
		//b.mutex.Unlock()
	}

	b.mutex.Lock()
	data := make([]byte, common.PageSize)
	err := b.diskManager.ReadPage(pageID, data)
	if err != nil {
		panic("ReadPage returned error!")
		//return nil
	}
	var pageData [common.PageSize]byte
	copy(pageData[:], data)
	pg := page.New(pageID, false, &pageData)
	b.pageTable[pageID] = *frameID
	b.pages[*frameID] = pg
	b.mutex.Unlock()

	return pg
}

// UnpinPage unpins the target page from the buffer pool.
func (b *BufferPoolManager) UnpinPage(pageID types.PageID, isDirty bool) error {
	b.mutex.Lock()

	if frameID, ok := b.pageTable[pageID]; ok {
		pg := b.pages[frameID]
		pg.DecPinCount()

		if pg.PinCount() <= 0 {
			(*b.replacer).Unpin(frameID)
		}

		if pg.IsDirty() || isDirty {
			pg.SetIsDirty(true)
		} else {
			pg.SetIsDirty(false)
		}

		b.mutex.Unlock()

		if common.EnableDebug {
			common.ShPrintf(common.DEBUG_INFO, "UnpinPage: PageId=%d PinCount=%d\n", pg.GetPageId(), pg.PinCount())
		}
		return nil
	}
	b.mutex.Unlock()

	//return errors.New("could not find page")
	panic("could not find page")
}

// FlushPage Flushes the target page to disk.
func (b *BufferPoolManager) FlushPage(pageID types.PageID) bool {
	b.mutex.Lock()
	if frameID, ok := b.pageTable[pageID]; ok {
		pg := b.pages[frameID]
		b.mutex.Unlock()
		pg.WLatch()
		pg.DecPinCount()

		data := pg.Data()
		b.diskManager.WritePage(pageID, data[:])
		pg.SetIsDirty(false)
		pg.WUnlatch()

		return true
	}
	b.mutex.Unlock()
	return false
}

// NewPage allocates a new page in the buffer pool with the disk manager help
func (b *BufferPoolManager) NewPage() *page.Page {
	frameID, isFromFreeList := b.getFrameID()
	if frameID == nil {
		return nil // the buffer is full, it can't find a frame
	}

	b.mutex.Lock()
	if !isFromFreeList {
		// remove page from current frame
		currentPage := b.pages[*frameID]
		if currentPage != nil {
			if currentPage.IsDirty() {
				b.log_manager.Flush()
				data := currentPage.Data()
				b.diskManager.WritePage(currentPage.ID(), data[:])
			}

			delete(b.pageTable, currentPage.ID())
		}
	}

	// allocates new page
	pageID := b.diskManager.AllocatePage()
	pg := page.NewEmpty(pageID)

	b.pageTable[pageID] = *frameID
	b.pages[*frameID] = pg

	b.mutex.Unlock()

	common.ShPrintf(common.DEBUG_INFO, "NewPage: returned pageID: %d\n", pageID)
	return pg
}

// DeletePage deletes a page from the buffer pool.
func (b *BufferPoolManager) DeletePage(pageID types.PageID) error {
	// 0.   Make sure you call DiskManager::DeallocatePage!
	// 1.   Search the page table for the requested page (P).
	// 1.   If P does not exist, return true.
	// 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
	// 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
	var frameID FrameID
	var ok bool
	b.mutex.Lock()
	if frameID, ok = b.pageTable[pageID]; !ok {
		b.mutex.Unlock()
		return nil
	}

	page := b.pages[frameID]
	page.WLatch()

	if page.PinCount() > 0 {
		page.WUnlatch()
		b.mutex.Unlock()
		return errors.New("Pin count greater than 0")
	}

	delete(b.pageTable, page.ID())
	(*b.replacer).Pin(frameID)
	//(*b.replacer).Unpin(frameID)
	b.diskManager.DeallocatePage(pageID)
	page.WUnlatch()

	b.freeList = append(b.freeList, frameID)
	b.mutex.Unlock()

	return nil
}

// FlushAllPages flushes all the pages in the buffer pool to disk.
func (b *BufferPoolManager) FlushAllPages() {
	pageIDs := make([]types.PageID, 0)
	b.mutex.Lock()
	for pageID, _ := range b.pageTable {
		pageIDs = append(pageIDs, pageID)
	}
	b.mutex.Unlock()

	for _, pageID := range pageIDs {
		b.FlushPage(pageID)
	}
}

// FlushAllDitryPages flushes all dirty pages in the buffer pool to disk.
func (b *BufferPoolManager) FlushAllDirtyPages() {
	pageIDs := make([]types.PageID, 0)
	b.mutex.Lock()
	for pageID, _ := range b.pageTable {
		if frameID, ok := b.pageTable[pageID]; ok {
			pg := b.pages[frameID]
			pg.RLatch()
			if pg.IsDirty() {
				pageIDs = append(pageIDs, pageID)
			}
			pg.RUnlatch()
		}
	}
	b.mutex.Unlock()

	for _, pageID := range pageIDs {
		b.FlushPage(pageID)
	}
}

func (b *BufferPoolManager) getFrameID() (*FrameID, bool) {
	b.mutex.Lock()
	if len(b.freeList) > 0 {
		frameID, newFreeList := b.freeList[0], b.freeList[1:]
		b.freeList = newFreeList

		b.mutex.Unlock()
		return &frameID, true
	}

	b.mutex.Unlock()
	return (*b.replacer).Victim(), false
}

func (b *BufferPoolManager) GetPages() []*page.Page {
	return b.pages
}

func (b *BufferPoolManager) GetPoolSize() int {
	return len(b.pageTable)
}

// NewBufferPoolManager returns a empty buffer pool manager
func NewBufferPoolManager(poolSize uint32, DiskManager disk.DiskManager, log_manager *recovery.LogManager) *BufferPoolManager {
	freeList := make([]FrameID, poolSize)
	pages := make([]*page.Page, poolSize)
	for i := uint32(0); i < poolSize; i++ {
		freeList[i] = FrameID(i)
		pages[i] = nil
	}

	replacer := NewClockReplacer(poolSize)
	return &BufferPoolManager{DiskManager, pages, replacer, freeList, make(map[types.PageID]FrameID), log_manager, new(sync.Mutex)}
}

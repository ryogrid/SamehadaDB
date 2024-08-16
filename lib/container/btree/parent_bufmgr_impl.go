package btree

import (
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"github.com/ryogrid/bltree-go-for-embedding/interfaces"
)

type ParentBufMgrImpl struct {
	*buffer.BufferPoolManager
}

func NewParentBufMgrImpl(bpm *buffer.BufferPoolManager) interfaces.ParentBufMgr {
	return &ParentBufMgrImpl{bpm}
}

func (p *ParentBufMgrImpl) FetchPPage(pageID int32) interfaces.ParentPage {
	return &ParentPageImpl{p.FetchPage(types.PageID(pageID))}
}

func (p *ParentBufMgrImpl) UnpinPPage(pageID int32, isDirty bool) error {
	return p.UnpinPage(types.PageID(pageID), isDirty)
}

func (p *ParentBufMgrImpl) NewPPage() interfaces.ParentPage {
	return &ParentPageImpl{p.NewPage()}
}

func (p *ParentBufMgrImpl) DeallocatePPage(pageID int32, isNoWait bool) error {
	return p.DeallocatePage(types.PageID(pageID), isNoWait)
}

package btree

import (
	"fmt"
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
	fmt.Println("ParentBufMgrImpl:FetchPPage pageID:", pageID)
	return &ParentPageImpl{p.FetchPage(types.PageID(pageID))}
}

func (p *ParentBufMgrImpl) UnpinPPage(pageID int32, isDirty bool) error {
	fmt.Println("ParentBufMgrImpl:UnpinPPage pageID:", pageID, "isDirty:", isDirty)
	return p.UnpinPage(types.PageID(pageID), isDirty)
}

func (p *ParentBufMgrImpl) NewPPage() interfaces.ParentPage {
	//fmt.Println("ParentBufMgrImpl:NewPPage")
	return &ParentPageImpl{p.NewPage()}
}

func (p *ParentBufMgrImpl) DeallocatePPage(pageID int32, isNoWait bool) error {
	return p.DeallocatePage(types.PageID(pageID), isNoWait)
}

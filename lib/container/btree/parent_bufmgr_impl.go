package btree

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"github.com/ryogrid/bltree-go-for-embedding/interfaces"
)

type ParentBufMgrImpl struct {
	*buffer.BufferPoolManager
	allocedPageNum int32
}

func NewParentBufMgrImpl(bpm *buffer.BufferPoolManager) interfaces.ParentBufMgr {
	return &ParentBufMgrImpl{bpm, 0}
}

func (p *ParentBufMgrImpl) FetchPPage(pageID int32) interfaces.ParentPage {
	fmt.Println("ParentBufMgrImpl:FetchPPage pageID:", pageID)
	p.allocedPageNum++
	fmt.Println("ParentBufMgrImpl:FetchPPage allocedPageNum:", p.allocedPageNum)
	return &ParentPageImpl{p.FetchPage(types.PageID(pageID))}
}

func (p *ParentBufMgrImpl) UnpinPPage(pageID int32, isDirty bool) error {
	fmt.Println("ParentBufMgrImpl:UnpinPPage pageID:", pageID, "isDirty:", isDirty)
	p.allocedPageNum--
	fmt.Println("ParentBufMgrImpl:UnpinPPage allocedPageNum:", p.allocedPageNum)
	return p.UnpinPage(types.PageID(pageID), isDirty)
}

func (p *ParentBufMgrImpl) NewPPage() interfaces.ParentPage {
	//fmt.Println("ParentBufMgrImpl:NewPPage")
	p.allocedPageNum++
	fmt.Println("ParentBufMgrImpl:NewPPage allocedPageNum:", p.allocedPageNum)
	return &ParentPageImpl{p.NewPage()}
}

func (p *ParentBufMgrImpl) DeallocatePPage(pageID int32, isNoWait bool) error {
	return p.DeallocatePage(types.PageID(pageID), isNoWait)
}

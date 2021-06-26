package table

import (
	"unsafe"

	"github.com/brunocalza/go-bustub/buffer"
	"github.com/brunocalza/go-bustub/storage/page"
)

type TableHeap struct {
	bpm         *buffer.BufferPoolManager
	firstPageId page.PageID
}

func NewTableHeap(bpm *buffer.BufferPoolManager) *TableHeap {
	p := bpm.NewPage()
	firstPage := (*TablePage)(unsafe.Pointer(p))
	firstPage.Init(p.ID(), -1)
	bpm.UnpinPage(p.ID(), true)
	return &TableHeap{bpm, p.ID()}
}

func (t *TableHeap) InsertTuple(tuple *Tuple, rid *page.RID) bool {
	currentPage := (*TablePage)(unsafe.Pointer(t.bpm.FetchPage(t.firstPageId)))

	for !currentPage.InsertTuple(tuple, rid) {
		nextPageId := currentPage.GetNextPageId()

		if nextPageId != -1 {
			t.bpm.UnpinPage(currentPage.GetTablePageId(), false)
			currentPage = (*TablePage)(unsafe.Pointer(t.bpm.FetchPage(nextPageId)))
		} else {
			p := t.bpm.NewPage()
			newPage := (*TablePage)(unsafe.Pointer(p))
			currentPage.SetNextPageId(p.ID())
			newPage.Init(p.ID(), currentPage.GetTablePageId())
			t.bpm.UnpinPage(currentPage.GetTablePageId(), true)
			currentPage = newPage
		}
	}

	t.bpm.UnpinPage(currentPage.GetTablePageId(), true)
	return true
}

func (t *TableHeap) GetTuple(rid *page.RID) *Tuple {
	page := (*TablePage)(unsafe.Pointer(t.bpm.FetchPage(rid.GetPageId())))
	//t.bpm.UnpinPage(page.I)
	return page.GetTuple(rid)
}

func (t *TableHeap) Begin() *TableIterator {
	var rid *page.RID
	pageId := t.firstPageId
	for pageId != -1 {
		page := (*TablePage)(unsafe.Pointer(t.bpm.FetchPage(pageId)))
		rid = page.GetTupleFirstRID()
		t.bpm.UnpinPage(pageId, false)
		if rid != nil {
			break
		}
		pageId = page.GetNextPageId()
	}
	return NewTableIterator(t, rid)
}

func (t *TableHeap) End() *TableIterator {
	rid := &page.RID{}
	rid.Set(-1, 0)
	return NewTableIterator(t, rid)
}

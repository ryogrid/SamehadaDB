package table

import (
	"unsafe"

	"github.com/brunocalza/go-bustub/storage/page"
)

type TableIterator struct {
	tableHeap *TableHeap
	tuple     *Tuple
}

func NewTableIterator(tableHeap *TableHeap, rid *page.RID) *TableIterator {
	tuple := tableHeap.GetTuple(rid)
	return &TableIterator{tableHeap, tuple}
}

func (it *TableIterator) Current() *Tuple {
	return it.tuple
}

func (it *TableIterator) Next() *Tuple {
	bpm := it.tableHeap.bpm
	currentPage := (*TablePage)(unsafe.Pointer(bpm.FetchPage(it.tuple.rid.GetPageId())))

	nextTupleRID := currentPage.getNextTupleRID(it.tuple.rid)
	if nextTupleRID == nil {
		for currentPage.getNextPageId().IsValid() {
			nextPage := (*TablePage)(unsafe.Pointer(bpm.FetchPage(currentPage.getNextPageId())))
			bpm.UnpinPage(currentPage.getTablePageId(), false)
			currentPage = nextPage
			nextTupleRID = currentPage.getNextTupleRID(it.tuple.rid)
			if nextTupleRID != nil {
				break
			}
		}
	}

	if nextTupleRID != nil && nextTupleRID.GetPageId().IsValid() {
		it.tuple = it.tableHeap.GetTuple(nextTupleRID)
	} else {
		it.tuple = nil
	}

	bpm.UnpinPage(currentPage.getTablePageId(), false)
	return it.tuple
}

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

func (i *TableIterator) Current() *Tuple {
	return i.tuple
}

func (i *TableIterator) Next() *Tuple {
	bpm := i.tableHeap.bpm
	currentPage := (*TablePage)(unsafe.Pointer(bpm.FetchPage(i.tuple.rid.GetPageId())))

	nextTupleRID := currentPage.GetNextTupleRID(i.tuple.rid)
	if nextTupleRID == nil {
		for currentPage.GetNextPageId() != -1 {
			nextPage := (*TablePage)(unsafe.Pointer(bpm.FetchPage(currentPage.GetNextPageId())))
			bpm.UnpinPage(currentPage.GetTablePageId(), false)
			currentPage = nextPage
			nextTupleRID = currentPage.GetNextTupleRID(i.tuple.rid)
			if nextTupleRID != nil {
				break
			}
		}
	}

	if nextTupleRID != nil && nextTupleRID.GetPageId() != -1 {
		i.tuple = i.tableHeap.GetTuple(nextTupleRID)
	} else {
		i.tuple = nil
	}

	bpm.UnpinPage(currentPage.GetTablePageId(), false)
	return i.tuple
}

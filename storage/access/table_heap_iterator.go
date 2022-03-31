// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package access

import (
	"fmt"

	"github.com/ryogrid/SamehadaDB/storage/tuple"
)

// TableHeapIterator is the access method for table heaps
//
// It iterates through a table heap when Next is called
// The tuple that it is being pointed to can be accessed with the method Current
type TableHeapIterator struct {
	tableHeap *TableHeap
	tuple     *tuple.Tuple
	txn       *Transaction
}

// NewTableHeapIterator creates a new table heap operator for the given table heap
// It points to the first tuple of the table heap
func NewTableHeapIterator(tableHeap *TableHeap, txn *Transaction) *TableHeapIterator {
	return &TableHeapIterator{tableHeap, tableHeap.GetFirstTuple(txn), txn}
}

// Current points to the current tuple
func (it *TableHeapIterator) Current() *tuple.Tuple {
	return it.tuple
}

// End checks if the iterator is at the end
func (it *TableHeapIterator) End() bool {
	return it.Current() == nil
}

// Next advances the iterator trying to find the next tuple
// The next tuple can be inside the same page of the current tuple
// or it can be in the next page
func (it *TableHeapIterator) Next() *tuple.Tuple {
	bpm := it.tableHeap.bpm
	currentPage := CastPageAsTablePage(bpm.FetchPage(it.tuple.GetRID().GetPageId()))

	nextTupleRID := currentPage.GetNextTupleRID(it.tuple.GetRID())
	if nextTupleRID != nil {
		fmt.Printf("nextTupleRID %d %d\n", nextTupleRID.PageId, nextTupleRID.SlotNum)
	} else {
		fmt.Println("nextTupleRID is nil!!!")
	}
	if nextTupleRID == nil {
		for currentPage.GetNextPageId().IsValid() {
			nextPage := CastPageAsTablePage(bpm.FetchPage(currentPage.GetNextPageId()))
			bpm.UnpinPage(currentPage.GetTablePageId(), false)
			currentPage = nextPage
			nextTupleRID = currentPage.GetNextTupleRID(it.tuple.GetRID())
			if nextTupleRID != nil {
				break
			}
		}
	}

	if nextTupleRID != nil && nextTupleRID.GetPageId().IsValid() {
		it.tuple = it.tableHeap.GetTuple(nextTupleRID, it.txn)
		fmt.Printf("branch1 at TableHeapIterator::Next() %v\n", it.tuple)
	} else {
		fmt.Println("branch2 at TableHeapIterator::Next()")
		it.tuple = nil
	}

	bpm.UnpinPage(currentPage.GetTablePageId(), false)
	return it.tuple
}

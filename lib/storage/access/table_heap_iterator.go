// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package access

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
)

// TableHeapIterator is the access method for table heaps
//
// It iterates through a table heap when Next is called
// The tuple1 that it is being pointed to can be accessed with the method Current
type TableHeapIterator struct {
	tableHeap    *TableHeap
	tuple        *tuple.Tuple
	lock_manager *LockManager
	txn          *Transaction
}

// NewTableHeapIterator creates a new table heap operator for the given table heap
// It points to the first tuple1 of the table heap
func NewTableHeapIterator(tableHeap *TableHeap, lock_manager *LockManager, txn *Transaction) *TableHeapIterator {
	return &TableHeapIterator{tableHeap, tableHeap.GetFirstTuple(txn), lock_manager, txn}
}

// Current points to the current tuple1
func (it *TableHeapIterator) Current() *tuple.Tuple {
	return it.tuple
}

// End checks if the iterator is at the end
func (it *TableHeapIterator) End() bool {
	return it.Current() == nil
}

// Next advances the iterator trying to find the next tuple1
// The next tuple1 can be inside the same page of the current tuple1
// or it can be in the next page
func (it *TableHeapIterator) Next() *tuple.Tuple {
start:
	bpm := it.tableHeap.bpm
	currentPage := CastPageAsTablePage(bpm.FetchPage(it.Current().GetRID().GetPageId()))
	currentPage.RLatch()

	nextTupleRID := currentPage.GetNextTupleRID(it.Current().GetRID(), false)
	if nextTupleRID == nil {
		// VARIANT: currentPage is always RLatched after loop
		for currentPage.GetNextPageId().IsValid() {
			nextPage := CastPageAsTablePage(bpm.FetchPage(currentPage.GetNextPageId()))
			if nextPage == nil {
				// TODO: (SDB) SHOULD BE FIXED: statics data update thread's call pass here rarely
				bpm.UnpinPage(currentPage.GetPageId(), false)
				currentPage.RUnlatch()
				it.tuple = nil
				return nil
			}
			bpm.UnpinPage(currentPage.GetPageId(), false)
			nextPage.RLatch()
			currentPage.RUnlatch()
			currentPage = nextPage
			nextTupleRID = currentPage.GetNextTupleRID(it.Current().GetRID(), true)

			if nextTupleRID != nil {
				break
			}
		}
	}

	finalizeCurrentPage := func() {
		bpm.UnpinPage(currentPage.GetPageId(), false)
		currentPage.RUnlatch()
	}

	var err error = nil
	if nextTupleRID != nil && nextTupleRID.GetPageId().IsValid() {
		it.tuple, err = currentPage.GetTuple(nextTupleRID, it.tableHeap.log_manager, it.lock_manager, it.txn)
		if it.tuple != nil && err == ErrSelfDeletedCase {
			fmt.Println("TableHeapIterator::Next ErrSelfDeletedCase!")
			finalizeCurrentPage()
			goto start
		}
	} else {
		it.tuple = nil
	}

	finalizeCurrentPage()
	return it.tuple
}

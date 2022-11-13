// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package access

import (
	"github.com/ryogrid/SamehadaDB/storage/tuple"
)

// TableHeapIterator is the access method for table heaps
//
// It iterates through a table heap when Next is called
// The tuple that it is being pointed to can be accessed with the method Current
type TableHeapIterator struct {
	tableHeap    *TableHeap
	tuple        *tuple.Tuple
	lock_manager *LockManager
	txn          *Transaction
}

// NewTableHeapIterator creates a new table heap operator for the given table heap
// It points to the first tuple of the table heap
func NewTableHeapIterator(tableHeap *TableHeap, lock_manager *LockManager, txn *Transaction) *TableHeapIterator {
	return &TableHeapIterator{tableHeap, tableHeap.GetFirstTuple(txn), lock_manager, txn}
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
	// TODO: (SDB) it.tuple access should be replaced to it.Current() after debugging
	currentPage := CastPageAsTablePage(bpm.FetchPage(it.tuple.GetRID().GetPageId()))
	currentPage.RLatch()

	// TODO: (SDB) it.tuple access should be replaced to it.Current() after debugging
	nextTupleRID := currentPage.GetNextTupleRID(it.tuple.GetRID(), false)
	if nextTupleRID == nil {
		// VARIANT: currentPage is always RLatched after loop
		for currentPage.GetNextPageId().IsValid() {
			nextPage := CastPageAsTablePage(bpm.FetchPage(currentPage.GetNextPageId()))
			currentPage.RUnlatch()
			currentPage.WLatch()
			bpm.UnpinPage(currentPage.GetPageId(), false)
			currentPage.WUnlatch()
			currentPage = nextPage
			currentPage.RLatch()
			nextTupleRID = currentPage.GetNextTupleRID(it.tuple.GetRID(), true)
			//nextTupleRID = currentPage.GetNextTupleRID(it.tuple.GetRID(), false)

			if nextTupleRID != nil {
				break
			}
		}
	}
	currentPage.RUnlatch()

	if nextTupleRID != nil && nextTupleRID.GetPageId().IsValid() {
		it.tuple = it.tableHeap.GetTuple(nextTupleRID, it.txn)
	} else {
		it.tuple = nil
	}

	currentPage.WLatch()
	bpm.UnpinPage(currentPage.GetPageId(), false)
	currentPage.WUnlatch()
	return it.tuple
}

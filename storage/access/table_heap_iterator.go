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
	currentPage := CastPageAsTablePage(bpm.FetchPage(it.Current().GetRID().GetPageId()))
	currentPage.RLatch()
	currentPage.AddRLatchRecord(int32(it.txn.txn_id))

	for {
		nextTupleRID := currentPage.GetNextTupleRID(it.Current().GetRID(), false)
		if nextTupleRID == nil {
			// VARIANT: currentPage is always RLatched after loop
			for currentPage.GetNextPageId().IsValid() {
				nextPage := CastPageAsTablePage(bpm.FetchPage(currentPage.GetNextPageId()))
				currentPage.RemoveRLatchRecord(int32(it.txn.txn_id))
				currentPage.RUnlatch()
				//currentPage.WLatch()
				//currentPage.AddWLatchRecord(int32(it.txn.txn_id))
				bpm.UnpinPage(currentPage.GetPageId(), false)
				//currentPage.RemoveWLatchRecord(int32(it.txn.txn_id))
				//currentPage.WUnlatch()
				currentPage = nextPage
				currentPage.RLatch()
				currentPage.AddRLatchRecord(int32(it.txn.txn_id))
				nextTupleRID = currentPage.GetNextTupleRID(it.Current().GetRID(), true)

				if nextTupleRID != nil {
					break
				}
			}
		}
		currentPage.RemoveRLatchRecord(int32(it.txn.txn_id))
		currentPage.RUnlatch()

		//currentPage.WLatch()
		//currentPage.AddWLatchRecord(int32(it.txn.txn_id))
		bpm.UnpinPage(currentPage.GetPageId(), false)
		//currentPage.RemoveWLatchRecord(int32(it.txn.txn_id))
		//currentPage.WUnlatch()

		var err error = nil
		if nextTupleRID != nil && nextTupleRID.GetPageId().IsValid() {
			it.tuple, err = it.tableHeap.GetTuple(nextTupleRID, it.txn)
			if it.tuple == nil && err == ErrSelfDeletedCase {
				continue
			}
			break
		} else {
			it.tuple = nil
			break
		}
	}

	return it.tuple
}

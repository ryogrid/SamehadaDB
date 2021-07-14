package table

// TableHeapIterator is the access method for table heaps
//
// It iterates through a table heap when Next is called
// The tuple that it is being pointed to can be accessed with the method Current
type TableHeapIterator struct {
	tableHeap *TableHeap
	tuple     *Tuple
}

// NewTableHeapIterator creates a new table heap operator for the given table heap
// It points to the first tuple of the table heap
func NewTableHeapIterator(tableHeap *TableHeap) *TableHeapIterator {
	return &TableHeapIterator{tableHeap, tableHeap.GetFirstTuple()}
}

// Current points to the current tuple
func (it *TableHeapIterator) Current() *Tuple {
	return it.tuple
}

// End checks if the iterator is at the end
func (it *TableHeapIterator) End() bool {
	return it.Current() == nil
}

// Next advances the iterator trying to find the next tuple
// The next tuple can be inside the same page of the current tuple
// or it can be in the next page
func (it *TableHeapIterator) Next() *Tuple {
	bpm := it.tableHeap.bpm
	currentPage := CastPageAsTablePage(bpm.FetchPage(it.tuple.rid.GetPageId()))

	nextTupleRID := currentPage.getNextTupleRID(it.tuple.rid)
	if nextTupleRID == nil {
		for currentPage.getNextPageId().IsValid() {
			nextPage := CastPageAsTablePage(bpm.FetchPage(currentPage.getNextPageId()))
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

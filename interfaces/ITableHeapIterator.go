package interfaces

type ITableHeapIterator interface {
	// NewTableHeapIterator creates a new table heap operator for the given table heap
	// It points to the first tuple of the table heap
	NewTableHeapIterator(tableHeap *ITableHeap) *ITableHeapIterator

	// Current points to the current tuple
	Current() *ITuple

	// End checks if the iterator is at the end
	End() bool

	// Next advances the iterator trying to find the next tuple
	// The next tuple can be inside the same page of the current tuple
	// or it can be in the next page
	Next() *ITuple
}

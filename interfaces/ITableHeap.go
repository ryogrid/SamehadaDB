package interfaces

import (
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/types"
)

type ITableHeap interface {
	// NewTableHeap creates a table heap without a transaction. (open table)
	NewTableHeap(bpm *buffer.BufferPoolManager) *ITableHeap

	// InitTableHeap ...
	InitTableHeap(bpm *buffer.BufferPoolManager, pageId types.PageID) *ITableHeap

	// GetFirstPageId returns firstPageId
	GetFirstPageId() types.PageID

	// InsertTuple inserts a tuple into the table
	//
	// It fetches the first page and tries to insert the tuple there.
	// If the tuple is too large (>= page_size):
	// 1. It tries to insert in the next page
	// 2. If there is no next page, it creates a new page and insert in it
	InsertTuple(tuple *ITuple, txn *ITransaction) (rid *page.RID, err error)

	// GetTuple reads a tuple from the table
	GetTuple(rid *page.RID) *ITuple

	// GetFirstTuple reads the first tuple from the table
	GetFirstTuple() *ITuple

	// Iterator returns a iterator for this table heap
	Iterator() *ITableHeapIterator
}

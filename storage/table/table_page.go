package table

import (
	"github.com/brunocalza/go-bustub/errors"
	"github.com/brunocalza/go-bustub/storage/page"
	"github.com/brunocalza/go-bustub/types"
)

const sizeTablePageHeader = uint32(24)
const sizeTuple = uint32(8)
const offsetPrevPageId = uint32(8)
const offsetNextPageId = uint32(12)
const offsetFreeSpace = uint32(16)
const offsetTupleCount = uint32(20)
const offsetTupleOffset = uint32(24)
const offsetTupleSize = uint32(28)

const ErrEmptyTuple = errors.Error("tuple cannot be empty")
const ErrNotEnoughSpace = errors.Error("there is not enough space")
const ErrNoFreeSlot = errors.Error("could not find a free slot")

// Slotted page format:
//  ---------------------------------------------------------
//  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
//  ---------------------------------------------------------
//                                ^
//                                free space pointer
//  Header format (size in bytes):
//  ----------------------------------------------------------------------------
//  | PageId (4)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
//  ----------------------------------------------------------------------------
//  ----------------------------------------------------------------
//  | TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
//  ----------------------------------------------------------------
type TablePage struct {
	page.Page
}

// Inserts a tuple into the table
func (tp *TablePage) InsertTuple(tuple *Tuple) (*page.RID, error) {
	if tuple.Size() == 0 {
		return nil, ErrEmptyTuple
	}

	if tp.getFreeSpaceRemaining() < tuple.Size()+sizeTuple {
		return nil, ErrNotEnoughSpace
	}

	var slot uint32

	// try to find a free slot
	for slot = uint32(0); slot < tp.getTupleCount(); slot++ {
		if tp.getTupleSize(slot) == 0 {
			break
		}
	}

	if tp.getTupleCount() == slot && tuple.Size()+sizeTuple > tp.getFreeSpaceRemaining() {
		return nil, ErrNoFreeSlot
	}

	tp.setFreeSpacePointer(tp.getFreeSpacePointer() - tuple.Size())
	tp.setTuple(slot, tuple)

	rid := &page.RID{}
	rid.Set(tp.getTablePageId(), slot)
	if slot == tp.getTupleCount() {
		tp.setTupleCount(tp.getTupleCount() + 1)
	}

	return rid, nil
}

// init initializes the table header
func (tp *TablePage) init(pageId types.PageID, prevPageId types.PageID) {
	tp.setPageId(pageId)
	tp.setPrevPageId(prevPageId)
	tp.setNextPageId(types.InvalidPageID)
	tp.setTupleCount(0)
	tp.setFreeSpacePointer(page.PageSize) // point to the end of the page
}

func (tp *TablePage) setPageId(pageId types.PageID) {
	tp.Copy(0, pageId.Serialize())
}

func (tp *TablePage) setPrevPageId(pageId types.PageID) {
	tp.Copy(offsetPrevPageId, pageId.Serialize())
}

func (tp *TablePage) setNextPageId(pageId types.PageID) {
	tp.Copy(offsetNextPageId, pageId.Serialize())
}

func (tp *TablePage) setFreeSpacePointer(freeSpacePointer uint32) {
	tp.Copy(offsetFreeSpace, types.UInt32(freeSpacePointer).Serialize())
}

func (tp *TablePage) setTupleCount(tupleCount uint32) {
	tp.Copy(offsetTupleCount, types.UInt32(tupleCount).Serialize())
}

func (tp *TablePage) setTuple(slot uint32, tuple *Tuple) {
	fsp := tp.getFreeSpacePointer()
	tp.Copy(fsp, tuple.data)                                                        // copy tuple to data starting at free space pointer
	tp.Copy(offsetTupleOffset+sizeTuple*slot, types.UInt32(fsp).Serialize())        // set tuple offset at slot
	tp.Copy(offsetTupleSize+sizeTuple*slot, types.UInt32(tuple.Size()).Serialize()) // set tuple size at slot
}

func (tp *TablePage) getTablePageId() types.PageID {
	return types.NewPageIDFromBytes(tp.Data()[:])
}

func (tp *TablePage) getNextPageId() types.PageID {
	return types.NewPageIDFromBytes(tp.Data()[offsetNextPageId:])
}

func (tp *TablePage) getTupleCount() uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offsetTupleCount:]))
}

func (tp *TablePage) getTupleOffsetAtSlot(slot uint32) uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offsetTupleOffset+sizeTuple*slot:]))
}

func (tp *TablePage) getTupleSize(slot uint32) uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offsetTupleSize+sizeTuple*slot:]))
}

func (tp *TablePage) getFreeSpaceRemaining() uint32 {
	return tp.getFreeSpacePointer() - sizeTablePageHeader - sizeTuple*tp.getTupleCount()
}

func (tp *TablePage) getFreeSpacePointer() uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offsetFreeSpace:]))
}

func (tp *TablePage) getTuple(rid *page.RID) *Tuple {
	slot := rid.GetSlot()
	tupleOffset := tp.getTupleOffsetAtSlot(slot)
	tupleSize := tp.getTupleSize(slot)

	tupleData := make([]byte, tupleSize)
	copy(tupleData, tp.Data()[tupleOffset:])

	return &Tuple{rid, tupleSize, tupleData}
}

func (tp *TablePage) getTupleFirstRID() *page.RID {
	firstRID := &page.RID{}

	tupleCount := tp.getTupleCount()
	for i := uint32(0); i < tupleCount; i++ {
		// if is deleted
		firstRID.Set(tp.getTablePageId(), i)
		return firstRID
	}
	return nil
}

func (tp *TablePage) getNextTupleRID(rid *page.RID) *page.RID {
	firstRID := &page.RID{}

	tupleCount := tp.getTupleCount()
	for i := rid.GetSlot() + 1; i < tupleCount; i++ {
		// if is deleted
		firstRID.Set(tp.getTablePageId(), i)
		return firstRID
	}
	return nil
}

package table

import (
	"unsafe"

	"github.com/brunocalza/go-bustub/errors"
	"github.com/brunocalza/go-bustub/storage/page"
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
func (tp *TablePage) init(pageId page.PageID, prevPageId page.PageID) {
	tp.setPageId(pageId)
	tp.setPrevPageId(prevPageId)
	tp.setNextPageId(page.InvalidID)
	tp.setTupleCount(0)
	tp.setFreeSpacePointer(page.PageSize) // point to the end of the page
}

func (tp *TablePage) getTupleOffsetAtSlot(slot uint32) uint32 {
	return *(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&tp.Data()[0])) + uintptr(offsetTupleOffset+sizeTuple*slot)))
}

func (tp *TablePage) getTupleSize(slot uint32) uint32 {
	return *(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&tp.Data()[0])) + uintptr(offsetTupleSize+sizeTuple*slot)))
}

func (tp *TablePage) getFreeSpaceRemaining() uint32 {
	return tp.getFreeSpacePointer() - sizeTablePageHeader - sizeTuple*tp.getTupleCount()
}

func (tp *TablePage) getFreeSpacePointer() uint32 {
	return *(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&tp.Data()[0])) + uintptr(offsetFreeSpace)))
}

func (tp *TablePage) setPageId(pageId page.PageID) {
	size := int32(unsafe.Sizeof(pageId))
	for i := int32(0); i < size; i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&tp.Data()[0])) + uintptr(i)))
		byt := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&pageId)) + uintptr(i)))
		*position = byt
	}
}

func (tp *TablePage) setPrevPageId(pageId page.PageID) {
	size := uint32(unsafe.Sizeof(pageId))
	for i := uint32(0); i < size; i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&tp.Data()[0])) + uintptr(offsetPrevPageId) + uintptr(i)))
		byt := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&pageId)) + uintptr(i)))
		*position = byt
	}
}

func (tp *TablePage) setNextPageId(pageId page.PageID) {
	size := uint32(unsafe.Sizeof(pageId))
	for i := uint32(0); i < size; i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&tp.Data()[0])) + uintptr(offsetNextPageId) + uintptr(i)))
		byt := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&pageId)) + uintptr(i)))
		*position = byt
	}
}

func (tp *TablePage) setFreeSpacePointer(freeSpacePointer uint32) {
	size := uint32(unsafe.Sizeof(freeSpacePointer))
	for i := uint32(0); i < size; i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&tp.Data()[0])) + uintptr(offsetFreeSpace+i)))
		byt := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&freeSpacePointer)) + uintptr(i)))
		*position = byt
	}
}

func (tp *TablePage) setTupleCount(tupleCount uint32) {
	size := uint32(unsafe.Sizeof(tupleCount))
	for i := uint32(0); i < size; i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&tp.Data()[0])) + uintptr(offsetTupleCount) + uintptr(i)))
		byt := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&tupleCount)) + uintptr(i)))
		*position = byt
	}
}

func (tp *TablePage) setTuple(slot uint32, tuple *Tuple) {
	// copy tuple to data starting at free space pointer
	for i := uint32(0); i < tuple.Size(); i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&tp.Data()[0])) + uintptr(tp.getFreeSpacePointer()+i)))
		*position = (*tuple.Data())[i]
	}

	// set tuple offset at slot
	tuplePosition := tp.getFreeSpacePointer()
	for i := uint32(0); i < uint32(unsafe.Sizeof(tuplePosition)); i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&tp.Data()[0])) + uintptr(offsetTupleOffset+sizeTuple*slot+i)))
		byt := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&tuplePosition)) + uintptr(i)))
		*position = byt
	}

	// set tuple size at slot
	tupleSize := tuple.Size()
	for i := uint32(0); i < uint32(unsafe.Sizeof(tupleSize)); i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&tp.Data()[0])) + uintptr(offsetTupleSize+sizeTuple*slot+i)))
		byt := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&tupleSize)) + uintptr(i)))
		*position = byt
	}
}

func (tp *TablePage) getTablePageId() page.PageID {
	return *(*page.PageID)(unsafe.Pointer(tp.Data()))
}

func (tp *TablePage) getNextPageId() page.PageID {
	return *(*page.PageID)(unsafe.Pointer(uintptr(unsafe.Pointer(&tp.Data()[0])) + uintptr(offsetNextPageId)))
}

func (tp *TablePage) getTupleCount() uint32 {
	return *(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&tp.Data()[0])) + uintptr(offsetTupleCount)))
}

func (tp *TablePage) getTuple(rid *page.RID) *Tuple {
	slot := rid.GetSlot()
	tupleOffset := tp.getTupleOffsetAtSlot(slot)
	tupleSize := tp.getTupleSize(slot)

	tupleData := make([]byte, tupleSize)

	for i := uint32(0); i < tupleSize; i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&tp.Data()[0])) + uintptr(tupleOffset+i)))
		tupleData[i] = *position
	}

	tuple := &Tuple{}
	tuple.size = tupleSize
	tuple.data = &tupleData
	tuple.rid = rid
	return tuple
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

package table

import (
	"unsafe"

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
	id       page.PageID
	pinCount int
	isDirty  bool
	data     *[page.PageSize]byte
}

func (t *TablePage) Init(pageId page.PageID, prevPageId page.PageID) {
	t.SetPageId(pageId)
	t.SetPrevPageId(prevPageId)
	t.SetNextPageId(-1)
	t.SetTupleCount(0)
	t.SetFreeSpacePointer(page.PageSize) // point to the end of the page
}

func (t *TablePage) InsertTuple(tuple *Tuple, rid *page.RID) bool {
	if t.getFreeSpaceRemaining() < tuple.Size()+sizeTuple {
		return false
	}

	var slot uint32
	for slot = 0; slot < t.GetTupleCount(); slot++ {
		if t.getTupleSize(slot) == 0 {
			break
		}
	}

	if t.GetTupleCount() == slot && t.getFreeSpaceRemaining() < tuple.Size()+sizeTuple {
		return false
	}

	t.SetFreeSpacePointer(t.getFreeSpacePointer() - tuple.Size())
	t.setTuple(slot, tuple)

	rid.Set(t.GetTablePageId(), slot)
	if slot == t.GetTupleCount() {
		t.SetTupleCount(t.GetTupleCount() + 1)
	}

	return true
}

func (t *TablePage) getTupleOffsetAtSlot(slot uint32) uint32 {
	return *(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&t.data[0])) + uintptr(offsetTupleOffset+sizeTuple*slot)))
}

func (t *TablePage) getTupleSize(slot uint32) uint32 {
	return *(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&t.data[0])) + uintptr(offsetTupleSize+sizeTuple*slot)))
}

func (t *TablePage) getFreeSpaceRemaining() uint32 {
	return t.getFreeSpacePointer() - sizeTablePageHeader - sizeTuple*t.GetTupleCount()
}

func (t *TablePage) getFreeSpacePointer() uint32 {
	return *(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&t.data[0])) + uintptr(offsetFreeSpace)))
}

func (t *TablePage) GetData() *[page.PageSize]byte {
	return t.data
}

func (t *TablePage) SetPageId(pageId page.PageID) {
	size := int(unsafe.Sizeof(pageId))
	for i := 0; i < size; i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&t.data[0])) + uintptr(i)))
		byt := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&pageId)) + uintptr(i)))
		*position = byt
	}
}

func (t *TablePage) SetPrevPageId(pageId page.PageID) {
	size := int(unsafe.Sizeof(pageId))
	for i := 0; i < size; i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&t.data[0])) + uintptr(offsetPrevPageId) + uintptr(i)))
		byt := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&pageId)) + uintptr(i)))
		*position = byt
	}
}

func (t *TablePage) SetNextPageId(pageId page.PageID) {
	size := int(unsafe.Sizeof(pageId))
	for i := 0; i < size; i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&t.data[0])) + uintptr(offsetNextPageId) + uintptr(i)))
		byt := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&pageId)) + uintptr(i)))
		*position = byt
	}
}

func (t *TablePage) SetFreeSpacePointer(freeSpacePointer uint32) {
	size := uint32(unsafe.Sizeof(freeSpacePointer))
	var i uint32
	for i = 0; i < size; i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&t.data[0])) + uintptr(offsetFreeSpace+i)))
		byt := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&freeSpacePointer)) + uintptr(i)))
		*position = byt
	}
}

func (t *TablePage) SetTupleCount(tupleCount uint32) {
	size := int(unsafe.Sizeof(tupleCount))
	for i := 0; i < size; i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&t.data[0])) + uintptr(offsetTupleCount) + uintptr(i)))
		byt := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&tupleCount)) + uintptr(i)))
		*position = byt
	}
}

func (t *TablePage) setTuple(slot uint32, tuple *Tuple) {
	var i uint32
	// copy tuple to data starting at free space pointer
	for i = 0; i < tuple.Size(); i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&t.data[0])) + uintptr(t.getFreeSpacePointer()+i)))
		*position = (*tuple.Data())[i]
	}

	// set tuple offset at slot
	tuplePosition := t.getFreeSpacePointer()
	for i = 0; i < uint32(unsafe.Sizeof(tuplePosition)); i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&t.data[0])) + uintptr(offsetTupleOffset+sizeTuple*slot+i)))
		byt := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&tuplePosition)) + uintptr(i)))
		*position = byt
	}

	// set tuple size at slot
	tupleSize := tuple.Size()
	for i = 0; i < uint32(unsafe.Sizeof(tupleSize)); i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&t.data[0])) + uintptr(offsetTupleSize+sizeTuple*slot+i)))
		byt := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&tupleSize)) + uintptr(i)))
		*position = byt
	}
}

func (t *TablePage) GetTablePageId() page.PageID {
	return *(*page.PageID)(unsafe.Pointer(t.data))
}

func (t *TablePage) GetPrevPageId() page.PageID {
	return *(*page.PageID)(unsafe.Pointer(uintptr(unsafe.Pointer(&t.data[0])) + uintptr(offsetPrevPageId)))
}

func (t *TablePage) GetNextPageId() page.PageID {
	return *(*page.PageID)(unsafe.Pointer(uintptr(unsafe.Pointer(&t.data[0])) + uintptr(offsetNextPageId)))
}

func (t *TablePage) GetTupleCount() uint32 {
	return *(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&t.data[0])) + uintptr(offsetTupleCount)))
}

func (t *TablePage) GetTuple(rid *page.RID) *Tuple {
	slot := rid.GetSlot()
	tupleOffset := t.getTupleOffsetAtSlot(slot)
	tupleSize := t.getTupleSize(slot)

	tupleData := make([]byte, tupleSize)

	var i uint32
	for i = 0; i < tupleSize; i++ {
		position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&t.data[0])) + uintptr(tupleOffset+i)))
		tupleData[i] = *position
	}

	tuple := &Tuple{}
	tuple.size = tupleSize
	tuple.data = &tupleData
	tuple.isAllocated = true
	tuple.rid = rid
	return tuple
}

func (t *TablePage) GetTupleFirstRID() *page.RID {
	firstRID := &page.RID{}

	var i uint32
	for i = 0; i < t.GetTupleCount(); i++ {
		// if is deleted
		firstRID.Set(t.GetTablePageId(), i)
		return firstRID
	}
	return nil
}

func (t *TablePage) GetNextTupleRID(rid *page.RID) *page.RID {
	firstRID := &page.RID{}

	var i uint32
	for i = rid.GetSlot() + 1; i < t.GetTupleCount(); i++ {
		// if is deleted
		firstRID.Set(t.GetTablePageId(), i)
		return firstRID
	}
	return nil
}

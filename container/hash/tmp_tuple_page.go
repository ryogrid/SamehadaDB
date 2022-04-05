package hash

import (
	"bytes"
	"encoding/binary"
	"unsafe"

	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

// To pass the test cases for this class, you must follow the existing TmpTuplePage format and implement the
// existing functions exactly as they are! It may be helpful to look at TablePage.
// Remember that this task is optional, you get full credit if you finish the next task.

/**
 * TmpTuplePage format:
 *
 * Sizes are in bytes.
 * | PageId (4) | LSN (4) | FreeSpace (4) | (free space) | TupleSize2 | TupleData2 | TupleSize1 | TupleData1 |
 *
 * We choose this format because DeserializeExpression expects to read Size followed by Data.
 */
type TmpTuplePage struct {
	*page.Page
}

// similar code learned from table_page.h/cpp  :)
func (p *TmpTuplePage) Init(page_id types.PageID, page_size uint32) {
	p.SetPageId(page_id)
	p.SetFreeSpacePointer(page_size)
}

func (p *TmpTuplePage) GetTablePageId() types.PageID { return p.GetData() }

func (p *TmpTuplePage) Insert(tuple_ *tuple.Tuple, out *TmpTuple) bool {
	free_offset := p.GetFreeSpacePointer()
	need_size := 4 + tuple_.Size()
	if free_offset-need_size < uint32(unsafe.Sizeof(*new(types.PageID))+unsafe.Sizeof(*new(types.LSN))+4) {
		return false
	}
	free_offset -= need_size
	p.SetFreeSpacePointer(free_offset)
	addr := p.GetNextPosToInsert()
	size := tuple_.Size()
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, size)
	copy(addr[:], buf.Bytes())
	data := tuple_.Data()
	copy(addr[int(unsafe.Sizeof(size)):], data[:tuple_.Size()])
	*out = NewTmpTuple(p.GetTablePageId(), p.GetOffset())
	return true
}

func (p *TmpTuplePage) Get(tuple_ *tuple.Tuple, offset uint32) {
	offset >= unsafe.Sizeof(*new(types.PageID))+unsafe.Sizeof(*new(types.LSN))+4
	tuple_.DeserializeFrom(p.GetData()[offset:])
}

func (p *TmpTuplePage) SetPageId(page_id types.PageID) {
	copy(p.GetData()[:], page_id.Serialize())
}
func (p *TmpTuplePage) GetFreeSpacePointer() uint32 {
	return p.GetData() + unsafe.Sizeof(*new(types.PageID)) + unsafe.Sizeof(*new(types.LSN))
}
func (p *TmpTuplePage) SetFreeSpacePointer(size uint32) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, size)
	sizeInBytes := buf.Bytes()
	copy(p.GetData()[unsafe.Sizeof(*new(types.PageID))+unsafe.Sizeof(*new(types.LSN)):], sizeInBytes)
}
func (p *TmpTuplePage) GetNextPosToInsert() []byte { return p.GetData()[p.GetFreeSpacePointer():] }
func (p *TmpTuplePage) GetOffset() uint32          { return p.GetFreeSpacePointer() }

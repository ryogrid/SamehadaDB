package hash

import (
	"bytes"
	"encoding/binary"
	"unsafe"

	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

const offsetFreeSpace = uint32(16)

/**
 * TmpTuplePage format:
 *
 * Sizes are in bytes.
 * | PageId (4) | LSN (4) | FreeSpace (4) | (free space) | TupleSize2 | TupleData2 | TupleSize1 | TupleData1 |
 *
 * We choose this format because DeserializeExpression expects to read Size followed by Data.
 */
type TmpTuplePage struct {
	page.Page
}

// CastPageAsTmpTuplePage casts the abstract Page struct into TmpTuplePage
func CastPageAsTmpTuplePage(page *page.Page) *TmpTuplePage {
	if page == nil {
		return nil
	}
	return (*TmpTuplePage)(unsafe.Pointer(page))
}

func (p *TmpTuplePage) Init(page_id types.PageID, page_size uint32) {
	p.SetPageId(page_id)
	p.SetFreeSpacePointer(page_size)
}

func (p *TmpTuplePage) GetTablePageId() types.PageID {
	return types.NewPageIDFromBytes(p.GetData()[:unsafe.Sizeof(*new(types.PageID))])
}

func (p *TmpTuplePage) Insert(tuple_ *tuple.Tuple, out *TmpTuple) bool {
	free_offset := p.GetFreeSpacePointer()
	need_size := 4 + tuple_.Size()

	if free_offset-need_size < uint32(offsetFreeSpace+4) {
		return false
	}
	free_offset -= need_size
	p.SetFreeSpacePointer(free_offset)
	addr := p.GetNextPosToInsert()
	size := tuple_.Size()
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, size)
	copy(addr[0:], buf.Bytes())
	data := tuple_.Data()
	copy(addr[int(unsafe.Sizeof(size)):], data[:tuple_.Size()])
	*out = *NewTmpTuple(p.GetTablePageId(), p.GetOffset())
	return true
}

func (p *TmpTuplePage) Get(tuple_ *tuple.Tuple, offset uint32) {
	tuple_.DeserializeFrom(p.GetData()[offset:])
}

func (p *TmpTuplePage) SetPageId(page_id types.PageID) {
	copy(p.GetData()[0:], page_id.Serialize())
}
func (p *TmpTuplePage) GetFreeSpacePointer() uint32 {
	return uint32(types.NewUInt32FromBytes(p.Data()[offsetFreeSpace:]))
}
func (p *TmpTuplePage) SetFreeSpacePointer(size uint32) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, size)
	sizeInBytes := buf.Bytes()
	copy(p.GetData()[offsetFreeSpace:], sizeInBytes)
}
func (p *TmpTuplePage) GetNextPosToInsert() []byte { return p.GetData()[p.GetFreeSpacePointer():] }
func (p *TmpTuplePage) GetOffset() uint32          { return p.GetFreeSpacePointer() }

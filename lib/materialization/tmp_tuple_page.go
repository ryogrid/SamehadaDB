package materialization

import (
	"bytes"
	"encoding/binary"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"unsafe"

	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

const offsetFreeSpace = uint32(16)

/**
 * TmpTuplePage format:
 *
 * Sizes are in bytes.
 * | PageID (4) | LSN (4) | FreeSpace (4) | (free space) | TupleSize2 | TupleData2 | TupleSize1 | TupleData1 |
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

func (p *TmpTuplePage) Init(pageID types.PageID, pageSize uint32) {
	p.SetPageID(pageID)
	p.SetFreeSpacePointer(pageSize)
}

func (p *TmpTuplePage) GetTablePageID() types.PageID {
	return types.NewPageIDFromBytes(p.GetData()[:unsafe.Sizeof(*new(types.PageID))])
}

func (p *TmpTuplePage) Insert(tpl *tuple.Tuple, out *TmpTuple) bool {
	freeOffset := p.GetFreeSpacePointer()
	needSize := 4 + tpl.Size()

	if freeOffset-needSize < uint32(offsetFreeSpace+4) {
		return false
	}
	freeOffset -= needSize
	p.SetFreeSpacePointer(freeOffset)
	addr := p.GetNextPosToInsert()
	size := tpl.Size()
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, size)
	copy(addr[0:], buf.Bytes())
	data := tpl.Data()
	copy(addr[int(unsafe.Sizeof(size)):], data[:tpl.Size()])
	*out = *NewTmpTuple(p.GetTablePageID(), p.GetOffset())
	return true
}

func (p *TmpTuplePage) Get(tpl *tuple.Tuple, offset uint32) {
	tpl.DeserializeFrom(p.GetData()[offset:])
}

func (p *TmpTuplePage) SetPageID(pageID types.PageID) {
	copy(p.GetData()[0:], pageID.Serialize())
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

func FetchTupleFromTmpTuplePage(bpm *buffer.BufferPoolManager, tpl *tuple.Tuple, tmpTuple *TmpTuple) {
	tmpPage := CastPageAsTmpTuplePage(bpm.FetchPage(tmpTuple.GetPageID()))
	if tmpPage == nil {
		panic("fail to fetch tmp page when doing hash join")
	}
	// tmpPage content is copied and accessed from currrent transaction only
	// so tuple locking is not needed
	tmpPage.Get(tpl, tmpTuple.GetOffset())
	bpm.UnpinPage(tmpTuple.GetPageID(), false)
}

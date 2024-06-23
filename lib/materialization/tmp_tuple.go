package materialization

import "github.com/ryogrid/SamehadaDB/lib/types"

type TmpTuple struct {
	page_id types.PageID
	offset  uint32
}

func NewTmpTuple(page_id types.PageID, offset uint32) *TmpTuple {
	return &TmpTuple{page_id, offset}
}

func (tt *TmpTuple) Equals(rhs *TmpTuple) bool {
	return tt.page_id == rhs.page_id && tt.offset == rhs.offset
}

func (tt *TmpTuple) GetPageId() types.PageID { return tt.page_id }
func (tt *TmpTuple) GetOffset() uint32       { return tt.offset }

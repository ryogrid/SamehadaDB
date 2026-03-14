package materialization

import "github.com/ryogrid/SamehadaDB/lib/types"

type TmpTuple struct {
	pageID types.PageID
	offset uint32
}

func NewTmpTuple(pageID types.PageID, offset uint32) *TmpTuple {
	return &TmpTuple{pageID, offset}
}

func (tt *TmpTuple) Equals(rhs *TmpTuple) bool {
	return tt.pageID == rhs.pageID && tt.offset == rhs.offset
}

func (tt *TmpTuple) GetPageID() types.PageID { return tt.pageID }
func (tt *TmpTuple) GetOffset() uint32       { return tt.offset }

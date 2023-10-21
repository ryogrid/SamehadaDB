package hash

import "github.com/ryogrid/SamehadaDB/types"

// when doing hash join
// we create a temp page to store all the tuple from left table which corresponds to the input key from right tuple
// so that we can refer to this page when calling Next from the hash join executor
type TmpTuple struct {
	page_id_ types.PageID
	offset_  uint32
}

func NewTmpTuple(page_id types.PageID, offset uint32) *TmpTuple {
	return &TmpTuple{page_id, offset}
}

func (tt *TmpTuple) Equals(rhs *TmpTuple) bool {
	return tt.page_id_ == rhs.page_id_ && tt.offset_ == rhs.offset_
}

func (tt *TmpTuple) GetPageId() types.PageID { return tt.page_id_ }
func (tt *TmpTuple) GetOffset() uint32       { return tt.offset_ }

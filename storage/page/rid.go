// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package page

import "github.com/ryogrid/SamehadaDB/types"

// RID is the record identifier for the given page identifier and slot number
type RID struct {
	PageId  types.PageID
	SlotNum uint32
}

// Set sets the recod identifier
func (r *RID) Set(pageId types.PageID, slot uint32) {
	r.PageId = pageId
	r.SlotNum = slot
}

// GetPageId gets the page id
func (r *RID) GetPageId() types.PageID {
	return r.PageId
}

// GetSlotNum gets the slot number
func (r *RID) GetSlotNum() uint32 {
	return r.SlotNum
}

// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package page

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

// RID is the record identifier for the given page identifier and slot number
type RID struct {
	PageID  types.PageID
	SlotNum uint32
}

// Set sets the recod identifier
func (r *RID) Set(pageID types.PageID, slot uint32) {
	r.PageID = pageID
	r.SlotNum = slot
}

// GetPageID gets the page id
func (r *RID) GetPageID() types.PageID {
	return r.PageID
}

// GetSlotNum gets the slot number
func (r *RID) GetSlotNum() uint32 {
	return r.SlotNum
}

func (r *RID) GetAsStr() string {
	return fmt.Sprintf("%v", *r)
}

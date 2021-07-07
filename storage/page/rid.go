package page

// RID is the record identifier for the given page identifier and slot number
type RID struct {
	pageId  PageID
	slotNum uint32
}

// Set sets the recod identifier
func (r *RID) Set(pageId PageID, slot uint32) {
	r.pageId = pageId
	r.slotNum = slot
}

// GetPageId gets the page id
func (r *RID) GetPageId() PageID {
	return r.pageId
}

// GetSlot gets the slot number
func (r *RID) GetSlot() uint32 {
	return r.slotNum
}

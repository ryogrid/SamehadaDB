package page

// Record Identifier for the given page identifier and slot number
type RID struct {
	pageId  PageID
	slotNum uint32
}

func (r *RID) Set(pageId PageID, slot uint32) {
	r.pageId = pageId
	r.slotNum = slot
}

func (r *RID) GetPageId() PageID {
	return r.pageId
}

func (r *RID) GetSlot() uint32 {
	return r.slotNum
}

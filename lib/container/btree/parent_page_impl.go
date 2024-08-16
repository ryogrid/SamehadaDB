package btree

import "github.com/ryogrid/SamehadaDB/lib/storage/page"

type ParentPageImpl struct {
	*page.Page
}

func (p *ParentPageImpl) DecPPinCount() {
	p.DecPinCount()
}

func (p *ParentPageImpl) PPinCount() int32 {
	return p.PinCount()
}

func (p *ParentPageImpl) GetPPageId() int32 {
	return int32(p.GetPageId())
}

func (p *ParentPageImpl) DataAsSlice() []byte {
	return (*p.Data())[:]
}

package page

// PageID is the type of the page identifier
type PageID int32

const PageSize = 4096

// Page represents a page on disk
type Page struct {
	id       PageID
	pinCount uint32
	isDirty  bool
	data     *[PageSize]byte
}

// IncPinCount decrements pin count
func (p *Page) IncPinCount() {
	p.pinCount++
}

// DecPinCount decrements pin count
func (p *Page) DecPinCount() {
	if p.pinCount > 0 {
		p.pinCount--
	}
}

// PinCount retunds the pin count
func (p *Page) PinCount() uint32 {
	return p.pinCount
}

// ID retunds the page id
func (p *Page) ID() PageID {
	return p.id
}

func (p *Page) Data() *[PageSize]byte {
	return p.data
}

func (p *Page) SetIsDirty(isDirty bool) {
	p.isDirty = isDirty
}

func (p *Page) IsDirty() bool {
	return p.isDirty
}

func (p *Page) CopyToData(data []byte) {
	for i := 0; i < len(data); i++ {
		p.data[i] = data[i]
	}
}

func New(id PageID, pinCount uint32, isDirty bool, data *[PageSize]byte) *Page {
	return &Page{id, pinCount, isDirty, data}
}

func NewEmpty(id PageID) *Page {
	return &Page{id, uint32(1), false, &[PageSize]byte{}}
}

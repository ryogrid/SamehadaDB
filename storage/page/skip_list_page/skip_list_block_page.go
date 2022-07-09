package skip_list_page

import (
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/types"
	"unsafe"
)

// TODO: (SDB) modify data layout described below
// Slotted page format:
//  ---------------------------------------------------------
//  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
//  ---------------------------------------------------------
//                                ^
//                                free space pointer
//  Header format (size in bytes):
//  ----------------------------------------------------------------------------
//  | PageId (4)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
//  ----------------------------------------------------------------------------
//  ----------------------------------------------------------------
//  | TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
//  ----------------------------------------------------------------

// TODO: (SDB) not implemented yet skip_list_block_page.go
const (
	DUMMY_MAX_ENTRY = 50
)

type SkipListBlockPageOnMem struct {
	//occuppied [(BlockArraySize-1)/8 + 1]byte // 256 bits
	//readable  [(BlockArraySize-1)/8 + 1]byte // 256 bits
	//array     [BlockArraySize]SkipListPair   // 252 * 16 bits
	key   []byte
	value uint32
}

type SkipListBlockPage struct {
	page.Page
	Level       int32
	SmallestKey types.Value
	Forward     []*SkipListBlockPage //[]types.PageID
	EntryCnt    int32
	MaxEntry    int32
	Entries     []*SkipListPair
	//occuppied [(BlockArraySize-1)/8 + 1]byte // 256 bits
	//readable  [(BlockArraySize-1)/8 + 1]byte // 256 bits
	//array     [BlockArraySize]SkipListPair   // 252 * 16 bits
}

func NewSkipListBlockPage(bpm *buffer.BufferPoolManager, level int32, smallestListPair *SkipListPair) *SkipListBlockPage {
	page_ := bpm.NewPage()
	if page_ == nil {
		return nil
	}

	ret := (*SkipListBlockPage)(unsafe.Pointer(page_))
	ret.Entries[0] = smallestListPair
	ret.EntryCnt = 1
	ret.MaxEntry = DUMMY_MAX_ENTRY
	ret.Forward = make([]*SkipListBlockPage, level)

	return ret
}

// if not found, returns info of nearest smaller key
// binary search is used for search
func (page_ *SkipListBlockPage) FindEntryByKey(key *types.Value) (found bool, entry *SkipListPair, index uint32) {
	//return page_.array[index].key
	return false, nil, 0
}

// Gets the key at an index in the block
func (page_ *SkipListBlockPage) KeyAt(index uint32) *types.Value {
	//return page_.array[index].key
	return nil
}

// Gets the value at an index in the block
func (page_ *SkipListBlockPage) ValueAt(index uint32) uint32 {
	//return page_.array[index].value
	return 0
}

// Attempts to insert a key and value into an index in the baccess.
func (page_ *SkipListBlockPage) Insert(index uint32, key uint32, value uint32) bool {
	//if page_.IsOccupied(index) {
	//	return false
	//}
	//
	//page_.array[index] = SkipListPair{key, value}
	//page_.occuppied[index/8] |= (1 << (index % 8))
	//page_.readable[index/8] |= (1 << (index % 8))
	return true
}

func (page_ *SkipListBlockPage) Remove(index uint32) {
	//if !page_.IsReadable(index) {
	//	return
	//}
	//
	//page_.readable[index/8] &= ^(1 << (index % 8))
}

// split entries of page_ at index
// new node contains entries after entry specified by index
func (page_ *SkipListBlockPage) SplitNode(index uint32) {
	//if !page_.IsReadable(index) {
	//	return
	//}
	//
	//page_.readable[index/8] &= ^(1 << (index % 8))
}

//// Returns whether or not an index is occuppied (valid key/value pair)
//func (page_ *SkipListBlockPage) IsOccupied(index uint32) bool {
//	return (page_.occuppied[index/8] & (1 << (index % 8))) != 0
//}
//
//// Returns whether or not an index is readable (valid key/value pair)
//func (page_ *SkipListBlockPage) IsReadable(index uint32) bool {
//	return (page_.readable[index/8] & (1 << (index % 8))) != 0
//}

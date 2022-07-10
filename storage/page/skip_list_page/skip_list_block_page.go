package skip_list_page

import (
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/types"
	"unsafe"
)

// TODO: (SDB) need to modify data layout figure
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
	ret.Entries = append(ret.Entries, smallestListPair)
	ret.EntryCnt = 1
	ret.MaxEntry = DUMMY_MAX_ENTRY
	ret.Forward = make([]*SkipListBlockPage, level)

	return ret
}

// Gets the entry at index in this node
func (node *SkipListBlockPage) EntryAt(idx int32) *SkipListPair {
	return node.Entries[idx]
}

// Gets the key at index in this node
func (node *SkipListBlockPage) KeyAt(idx int32) *types.Value {
	key := node.Entries[idx].Key
	return &key
}

// Gets the value at an index in this node
func (node *SkipListBlockPage) ValueAt(idx int32) uint32 {
	val := node.Entries[idx].Value
	return val
}

// if not found, returns info of nearest smaller key
// binary search is used for search
func (node *SkipListBlockPage) FindEntryByKey(key *types.Value) (found bool, entry *SkipListPair, index int32) {
	leftIdx := int32(-1)
	rightIdx := node.EntryCnt
	curIdx := int32(-1)
	var curEntry *SkipListPair
	for 1 < rightIdx-leftIdx {
		curIdx := (leftIdx + rightIdx) / 2
		curEntry := node.EntryAt(curIdx)
		if curEntry.Key.CompareLessThan(*key) {
			leftIdx = curIdx
		} else {
			rightIdx = curIdx
		}
	}
	//return right
	if leftIdx == rightIdx {
		return true, curEntry, curIdx
	} else {
		if curEntry.Key.CompareLessThan(*key) {
			return false, curEntry, curIdx
		} else {
			return false, node.EntryAt(curIdx - 1), curIdx - 1
		}
	}
}

// Attempts to insert a key and value into an index in the baccess
// return value is whether newNode is created or not
func (node *SkipListBlockPage) Insert(key *types.Value, value uint32, bpm *buffer.BufferPoolManager, skipPathList []*SkipListBlockPage,
	level int32, startNode *SkipListBlockPage) bool {
	found, _, foundIdx := node.FindEntryByKey(key)
	isMadeNewNode := false
	if found {
		// over write exsiting entry
		node.Entries[foundIdx] = &SkipListPair{*key, value}
		return isMadeNewNode
	} else if !found {
		if node.EntryCnt+1 > node.MaxEntry {
			// this node is full. so node split is needed

			// first, split this node at center of entry list
			// half of entries are moved to new node
			splitIdx := node.MaxEntry / 2
			// update with this node
			skipPathList[0] = node
			node.SplitNode(splitIdx, bpm, skipPathList, level, startNode)
			isMadeNewNode = true

			if foundIdx > splitIdx {
				// insert to new node
				newSmallerIdx := foundIdx - splitIdx
				newNode := node.Forward[0]
				newNode.Entries = append(newNode.Entries[:newSmallerIdx+1+1], newNode.Entries[newSmallerIdx+1:]...)
				newNode.Entries[newSmallerIdx+1] = &SkipListPair{*key, value}

				return isMadeNewNode
			} // else => insert to this node
		}
		// insert to this node
		// foundIdx is index of nearlest smaller key entry
		// new entry is inserted next of nearlest smaller key entry
		node.Entries = append(node.Entries[:foundIdx+1+1], node.Entries[foundIdx+1:]...)
		node.Entries[foundIdx+1] = &SkipListPair{*key, value}
		if foundIdx == -1 {
			node.SmallestKey = *key
		}
	}
	return isMadeNewNode
}

func (node *SkipListBlockPage) Remove(key *types.Value) {
	found, _, foundIdx := node.FindEntryByKey(key)
	if found && (node.EntryCnt == 1) {
		// when there are no enry without target entry
		// this node keep reft with no entry (but new entry can be stored)

		// delete specified entry
		node.Entries[0] = nil

		var smallestKey types.Value
		switch key.ValueType() {
		case types.Integer:
			smallestKey = types.NewInteger(0)
			smallestKey.SetInfMin()
		case types.Float:
			smallestKey = types.NewFloat(0)
			smallestKey.SetInfMin()
		case types.Varchar:
			smallestKey = types.NewVarchar("")
			smallestKey.SetInfMin()
		case types.Boolean:
			smallestKey = types.NewBoolean(false)
			smallestKey.SetInfMin()
		default:
			panic("not implemented")
		}
		node.SmallestKey = smallestKey
	} else if found {
		node.Entries = append(node.Entries[:foundIdx], node.Entries[foundIdx+1:]...)
	} else { // found == false
		// do nothing
	}
}

// TODO: (SDB) not implemented (SplitNode)

// split entries of node at entry specified with idx arg
// new node contains entries node.Entries[idx+1:]
// (new node does not include entry node.Entries[idx])
func (node *SkipListBlockPage) SplitNode(idx int32, bpm *buffer.BufferPoolManager, skipPathList []*SkipListBlockPage,
	level int32, startNode *SkipListBlockPage) {

	newNode := NewSkipListBlockPage(bpm, level, nil)
	newNode.Entries = node.Entries[idx+1:]
	newNode.SmallestKey = newNode.Entries[0].Key
	newNode.EntryCnt = int32(len(newNode.Entries))

	skipPathList[level-1] = startNode
	for ii := int32(0); ii < level; ii++ {
		newNode.Forward[ii] = skipPathList[ii].Forward[ii]
		skipPathList[ii].Forward[ii] = newNode
	}
}

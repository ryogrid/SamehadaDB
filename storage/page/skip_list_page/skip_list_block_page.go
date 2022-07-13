package skip_list_page

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/types"
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

	ret := new(SkipListBlockPage)
	ret.Page = *page_
	//(*SkipListBlockPage)(unsafe.Pointer(page_))
	ret.Entries = make([]*SkipListPair, 0) // for first insert works
	ret.Entries = append(ret.Entries, smallestListPair)
	ret.SmallestKey = smallestListPair.Key
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
// https://www.cs.usfca.edu/~galles/visualization/Search.html
func (node *SkipListBlockPage) FindEntryByKey(key *types.Value) (found bool, entry *SkipListPair, index int32) {
	if node.EntryCnt == 1 {
		// when first entry at start node
		return false, node.Entries[0], 0
	} else {
		lowIdx := int32(0)
		highIdx := node.EntryCnt - 1
		midIdx := int32(-1)

		for lowIdx <= highIdx {
			midIdx = (lowIdx + highIdx) / 2
			if node.KeyAt(midIdx).CompareEquals(*key) {
				return true, node.EntryAt(midIdx), midIdx
			} else if node.KeyAt(midIdx).CompareLessThan(*key) {
				lowIdx = midIdx + 1
			} else {
				highIdx = midIdx - 1
			}
		}
		if lowIdx < highIdx {
			return false, node.EntryAt(lowIdx), lowIdx
		} else {
			if highIdx < 0 {
				return false, node.EntryAt(0), 0
			} else {
				return false, node.EntryAt(highIdx), highIdx
			}
		}
	}

}

// Attempts to insert a key and value into an index in the baccess
// return value is whether newNode is created or not
func (node *SkipListBlockPage) Insert(key *types.Value, value uint32, bpm *buffer.BufferPoolManager, skipPathList []*SkipListBlockPage,
	level int32, curMaxLevel int32, startNode *SkipListBlockPage) bool {
	fmt.Printf("Insert of SkipListBlockPage called! : page=%v key=%d\n", node.ID(), key.ToInteger())

	found, _, foundIdx := node.FindEntryByKey(key)
	isMadeNewNode := false
	if found {
		fmt.Println("found at Insert")
		// over write exsiting entry
		node.Entries[foundIdx] = &SkipListPair{*key, value}
		fmt.Printf("end of Insert of SkipListBlockPage called! : page=%v key=%d page.EntryCnt=%d len(page.Entries)=%d\n", node.ID(), key.ToInteger(), node.EntryCnt, len(node.Entries))
		return isMadeNewNode
	} else if !found {
		fmt.Printf("not found at Insert of SkipListBlockPage. foundIdx=%d\n", foundIdx)
		if node.EntryCnt+1 > node.MaxEntry {
			// this node is full. so node split is needed

			// first, split this node at center of entry list
			// half of entries are moved to new node
			splitIdx := node.MaxEntry / 2
			// update with this node
			skipPathList[0] = node
			node.SplitNode(splitIdx, bpm, skipPathList, level, curMaxLevel, startNode)
			isMadeNewNode = true

			if foundIdx >= splitIdx {
				// insert to new node
				newSmallerIdx := foundIdx - splitIdx + 1
				newNode := node.Forward[0]
				if (newSmallerIdx + 1) >= newNode.EntryCnt {
					// when insertin point is next of last entry of new node
					common.SH_Assert(node.Entries[len(newNode.Entries)-1].Key.CompareLessThan(*key), "order is invalid.")
					newNode.Entries = append(newNode.Entries, &SkipListPair{*key, value})
					//var rightEntry []*SkipListPair = nil
					//if newSmallerIdx < newNode.EntryCnt-1 {
					//	rightEntry = newNode.Entries[newSmallerIdx+1:]
					//}
					//
					//if newSmallerIdx+1 >= newNode.EntryCnt {
					//	newNode.Entries = append(newNode.Entries, &SkipListPair{*key, value})
					//} else {
					//	newNode.Entries = append(newNode.Entries[:newSmallerIdx+1], &SkipListPair{*key, value})
					//}
					//
					//if rightEntry != nil {
					//	newNode.Entries = append(newNode.Entries, rightEntry...)
					//}
				} else {
					newNode.Entries = append(newNode.Entries[:newSmallerIdx], newNode.Entries[newSmallerIdx-1:]...)
					//common.SH_Assert(newNode.Entries[newSmallerIdx-2].Key.CompareLessThan(*key), "order is invalid.")
					newNode.Entries[newSmallerIdx-1] = &SkipListPair{*key, value}
				}
				//newNode.Entries = append(newNode.Entries[:newSmallerIdx+1+1], newNode.Entries[newSmallerIdx+1:]...)
				//newNode.Entries[newSmallerIdx+1] = &SkipListPair{*key, value}
				newNode.EntryCnt = int32(len(newNode.Entries))

				fmt.Printf("end of Insert of SkipListBlockPage called! : page=%v key=%d page.EntryCnt=%d len(page.Entries)=%d\n", node.ID(), key.ToInteger(), node.EntryCnt, len(node.Entries))

				return isMadeNewNode
			} // else => insert to this node
		}
		// insert to this node
		// foundIdx is index of nearlest smaller key entry
		// new entry is inserted next of nearlest smaller key entry

		if (foundIdx + 1) >= node.EntryCnt {
			// when inserting point is next of last entry of this node
			common.SH_Assert(node.Entries[len(node.Entries)-1].Key.IsInfMin() || node.Entries[len(node.Entries)-1].Key.CompareLessThan(*key), "order is invalid.")
			node.Entries = append(node.Entries, &SkipListPair{*key, value})

			//var rightEntry []*SkipListPair = nil
			//if foundIdx < node.EntryCnt-1 {
			//	rightEntry = node.Entries[foundIdx+1:]
			//}
			//node.Entries = append(node.Entries[:foundIdx+1], &SkipListPair{*key, value})
			//if rightEntry != nil {
			//	node.Entries = append(node.Entries, rightEntry...)
			//}
		} else {
			node.Entries = append(node.Entries[:foundIdx+1+1], node.Entries[foundIdx+1:]...)
			common.SH_Assert(node.Entries[foundIdx].Key.CompareLessThan(*key), "order is invalid.")
			node.Entries[foundIdx+1] = &SkipListPair{*key, value}
		}
		node.EntryCnt = int32(len(node.Entries))
		//	if foundIdx == -1 {
		//		node.SmallestKey = *key
		//	}
		//}
	}
	fmt.Printf("end of Insert of SkipListBlockPage called! : page=%v key=%d page.EntryCnt=%d len(page.Entries)=%d\n", node.ID(), key.ToInteger(), node.EntryCnt, len(node.Entries))
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

// split entries of node at entry specified with idx arg
// new node contains entries node.Entries[idx+1:]
// (new node does not include entry node.Entries[idx])
func (node *SkipListBlockPage) SplitNode(idx int32, bpm *buffer.BufferPoolManager, skipPathList []*SkipListBlockPage,
	level int32, curMaxLevel int32, startNode *SkipListBlockPage) {
	fmt.Println("SplitNode called!")

	newNode := NewSkipListBlockPage(bpm, level, node.Entries[idx+1])
	newNode.Entries = append(make([]*SkipListPair, 0), node.Entries[idx+1:]...)
	newNode.SmallestKey = newNode.Entries[0].Key
	newNode.EntryCnt = int32(len(newNode.Entries))
	newNode.Level = level
	node.Entries = node.Entries[:idx+1]
	node.EntryCnt = int32(len(node.Entries))

	if level > curMaxLevel {
		skipPathList[level-1] = startNode
	}
	for ii := int32(0); ii < level; ii++ {
		newNode.Forward[ii] = skipPathList[ii].Forward[ii]
		skipPathList[ii].Forward[ii] = newNode
	}
}

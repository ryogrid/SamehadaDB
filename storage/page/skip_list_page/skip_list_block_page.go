package skip_list_page

import (
	"bytes"
	"encoding/binary"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/types"
	"unsafe"
)

// Slotted page format:
//  ---------------------------------------------------------
//  | HEADER | ... FREE SPACE ... | ... INSERTED ENTRIES ... |
//  ---------------------------------------------------------
//                                ^
//                                free space pointer
//
//  Header format (size in bytes):
//  -------------------------------------------------------------------------------------------------------------------
//  | PageId (4)| level (4)| entryCnt (4)| isNeedDeleted (1)| forward (4 * MAX_FOWARD_LIST_LEN) | FreeSpacePointer(4) |
//  ------------------------------------------------------------------------------------------------------------------
//  -------------------------------------------------------------
//  | Entry_1 offset (4) | Entry_1 size (4) | ..................|
//  ------------------------------------------------------------
//
//  Entries format (size in bytes):
//  -----------------------------------------------------------------------------------------------------------------------------
//  | HEADER | ... FREE SPACE ... | ...............| Entry2_key (1+x) | Entry_2 value (4) | Entry1_key (1+x) | Entry_1 value (4)|
//  ----------------------------------------------------------------------------------------------------------------------------
//                                                ^ <-------------- size ---------------> ^ <-------------- size --------------->
//                                         offset(from buffer head)                   offset(...)
//  Entry_key format (size in bytes)
//    = Serialized types.Value
//  ----------------------------------------------
//  | isNull (1) | data according to KeyType (x) |
//  ----------------------------------------------

const (
	DUMMY_MAX_ENTRY        = 10 //50
	sizePageId             = uint32(4)
	sizeLevel              = uint32(4)
	sizeEntryCnt           = uint32(4)
	sizeIsNeedDeleted      = uint32(1)
	sizeForward            = uint32(4 * MAX_FOWARD_LIST_LEN)
	sizeForwardEntry       = uint32(4) // types.PageID
	sizeFreeSpacePointer   = uint32(4)
	sizeTablePageHeader    = sizePageId + sizeLevel + sizeEntryCnt + sizeIsNeedDeleted + sizeForward + sizeFreeSpacePointer
	offsetPageId           = int32(0)
	offsetLevel            = sizePageId
	offsetEntryCnt         = offsetLevel + sizeLevel
	offsetIsNeedDeleted    = offsetEntryCnt + sizeEntryCnt
	offsetForward          = offsetIsNeedDeleted + sizeIsNeedDeleted
	offsetFreeSpacePointer = offsetForward + sizeForward
	sizeEntryValue         = uint32(4)
)

type SkipListPair struct {
	Key   types.Value
	Value uint32
}

func (sp SkipListPair) Serialize() []byte {
	// TODO: (SDB) not implemented yet (SkipListPair::Serialize)
	return nil
}

func NewSkpListPairFromBytes(size uint32, keyType types.TypeID) *SkipListPair {
	// TODO: (SDB) not implemented yet (SkipListPair::NewSkipListPairFromBytes)
	return nil
}

type SkipListBlockPage struct {
	page.Page
	//level    int32
	//entryCnt int32
	////maxEntry      int32
	//isNeedDeleted bool
	//forward       [MAX_FOWARD_LIST_LEN]types.PageID //*SkipListBlockPage
	//entries       []SkipListPair
	////smallestKey   types.Value
}

// TODO: (SDB) caller should UnpinPage with appropriate dirty flag call to returned object
func NewSkipListBlockPage(bpm *buffer.BufferPoolManager, level int32, smallestListPair SkipListPair) *SkipListBlockPage {
	page_ := bpm.NewPage()
	if page_ == nil {
		return nil
	}

	//ret := new(SkipListBlockPage)
	ret := (*SkipListBlockPage)(unsafe.Pointer(page_))
	ret.SetPageId(page_.ID())
	ret.SetEntries(make([]SkipListPair, 0)) // for first insert works
	ret.SetEntries(append(ret.GetEntries(), smallestListPair))
	ret.SetSmallestKey(smallestListPair.Key)
	ret.SetEntryCnt(1)
	//ret.SetMaxEntry(DUMMY_MAX_ENTRY)
	//ret.SetForward(make([]*SkipListBlockPage, level))
	//ret.Backward = make([]*SkipListBlockPage, level)

	return ret
}

// Gets the entry at index in this node
func (node *SkipListBlockPage) EntryAt(idx int32) SkipListPair {
	return node.GetEntry(idx)
}

// Gets the key at index in this node
func (node *SkipListBlockPage) KeyAt(idx int32) *types.Value {
	key := node.GetEntry(idx).Key
	return &key
}

// Gets the value at an index in this node
func (node *SkipListBlockPage) ValueAt(idx int32) uint32 {
	val := node.GetEntry(idx).Value
	return val
}

// if not found, returns info of nearest smaller key
// binary search is used for search
// https://www.cs.usfca.edu/~galles/visualization/Search.html
func (node *SkipListBlockPage) FindEntryByKey(key *types.Value) (found bool, entry SkipListPair, index int32) {
	if node.GetEntryCnt() == 1 {
		if node.GetEntry(0).Key.CompareEquals(*key) {
			return true, node.GetEntry(0), 0
		} else {
			if node.GetEntry(0).Key.IsInfMin() {
				return false, node.GetEntry(0), 0
			} else {
				if node.GetEntry(0).Key.CompareLessThan(*key) {
					return false, node.GetEntry(0), 0
				} else {
					return false, node.GetEntry(0), -1
				}

			}
		}
	} else {
		lowIdx := int32(0)
		highIdx := node.GetEntryCnt() - 1
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
	//fmt.Printf("Insert of SkipListBlockPage called! : key=%d\n", key.ToInteger())

	found, _, foundIdx := node.FindEntryByKey(key)
	isMadeNewNode := false
	var splitIdx int32 = -1
	if found {
		//fmt.Println("found at Insert")
		// over write exsiting entry
		if !node.GetEntry(foundIdx).Key.CompareEquals(*key) {
			panic("overwriting wrong value!")
		}

		node.SetEntry(foundIdx, SkipListPair{*key, value})
		//fmt.Printf("end of Insert of SkipListBlockPage called! : key=%d page.entryCnt=%d len(page.entries)=%d\n", key.ToInteger(), node.entryCnt, len(node.entries))
		return isMadeNewNode
	} else if !found {
		//fmt.Printf("not found at Insert of SkipListBlockPage. foundIdx=%d\n", foundIdx)
		if node.GetEntryCnt()+1 > DUMMY_MAX_ENTRY {
			// this node is full. so node split is needed

			// first, split this node at center of entry list
			// half of entries are moved to new node
			splitIdx = DUMMY_MAX_ENTRY / 2
			// update with this node
			skipPathList[0] = node
			node.SplitNode(splitIdx, bpm, skipPathList, level, curMaxLevel, startNode)
			isMadeNewNode = true

			if foundIdx > splitIdx {
				// insert to new node
				newSmallerIdx := foundIdx - splitIdx - 1
				newNodePageId := node.GetForwardEntry(0)
				page_ := bpm.FetchPage(newNodePageId)
				newNode := (*SkipListBlockPage)(unsafe.Pointer(page_))

				if (newSmallerIdx + 1) >= newNode.GetEntryCnt() {
					// when inserting point is next of last entry of new node
					common.SH_Assert(newNode.GetEntry(int32(len(newNode.GetEntries())-1)).Key.CompareLessThan(*key), "order is invalid.")
					copiedEntries := make([]SkipListPair, len(newNode.GetEntries()))
					copy(copiedEntries, newNode.GetEntries()[:])
					newNode.SetEntries(append(copiedEntries, SkipListPair{*key, value}))
					//newNode.entries = append(newNode.entries, &SkipListPair{*key, value})
				} else {
					formerEntries := make([]SkipListPair, len(newNode.GetEntries()[:newSmallerIdx+1]))
					copy(formerEntries, newNode.GetEntries()[:newSmallerIdx+1])
					laterEntries := make([]SkipListPair, len(newNode.GetEntries()[newSmallerIdx+1:]))
					copy(laterEntries, newNode.GetEntries()[newSmallerIdx+1:])
					formerEntries = append(formerEntries, SkipListPair{*key, value})
					formerEntries = append(formerEntries, laterEntries...)
					newNode.SetEntries(formerEntries)
				}
				newNode.SetSmallestKey(newNode.GetEntry(0).Key)
				newNode.SetEntryCnt(int32(len(newNode.GetEntries())))

				//fmt.Printf("end of Insert of SkipListBlockPage called! : key=%d page.entryCnt=%d len(page.entries)=%d\n", key.ToInteger(), node.entryCnt, len(node.entries))

				return isMadeNewNode
			} // else => insert to this node
		}
		// insert to this node
		// foundIdx is index of nearlest smaller key entry
		// new entry is inserted next of nearlest smaller key entry

		var toSetEntryCnt int32 = -1
		if (foundIdx + 1) >= node.GetEntryCnt() {
			// when inserting point is next of last entry of this node
			common.SH_Assert(node.GetEntry(int32(len(node.GetEntries())-1)).Key.IsInfMin() || node.GetEntry(int32(len(node.GetEntries())-1)).Key.CompareLessThan(*key), "order is invalid.")
			copiedEntries := make([]SkipListPair, len(node.GetEntries()))
			copy(copiedEntries, node.GetEntries()[:])
			node.SetEntries(append(copiedEntries, SkipListPair{*key, value}))
			toSetEntryCnt = int32(len(copiedEntries) + 1)
			//node.entries = append(node.entries, SkipListPair{*key, value})
		} else {
			if foundIdx == -1 {
				var copiedEntries []SkipListPair = nil
				if isMadeNewNode {
					copiedEntries = make([]SkipListPair, len(node.GetEntries()[:splitIdx+1]))
					copy(copiedEntries, node.GetEntries()[:splitIdx+1])
				} else {
					copiedEntries = make([]SkipListPair, len(node.GetEntries()[:]))
					copy(copiedEntries, node.GetEntries()[:])
				}
				finalEntriles := make([]SkipListPair, 0)
				finalEntriles = append(finalEntriles, SkipListPair{*key, value})
				node.SetEntries(append(finalEntriles, copiedEntries...))
				toSetEntryCnt = int32(len(finalEntriles) + len(copiedEntries))
			} else {
				formerEntries := make([]SkipListPair, len(node.GetEntries()[:foundIdx+1]))
				copy(formerEntries, node.GetEntries()[:foundIdx+1])
				var laterEntries []SkipListPair = nil
				if isMadeNewNode {
					laterEntries = make([]SkipListPair, len(node.GetEntries()[foundIdx+1:splitIdx+1]))
					copy(laterEntries, node.GetEntries()[foundIdx+1:splitIdx+1])
				} else {
					laterEntries = make([]SkipListPair, len(node.GetEntries()[foundIdx+1:]))
					copy(laterEntries, node.GetEntries()[foundIdx+1:])
				}

				formerEntries = append(formerEntries, SkipListPair{*key, value})
				formerEntries = append(formerEntries, laterEntries...)
				node.SetEntries(formerEntries)
				toSetEntryCnt = int32(len(formerEntries))
			}
		}
		node.SetSmallestKey(node.GetEntry(0).Key)
		//node.SetEntryCnt(int32(len(node.GetEntries())))
		node.SetEntryCnt(toSetEntryCnt)
	}
	//fmt.Printf("end of Insert of SkipListBlockPage called! : key=%d page.entryCnt=%d len(page.entries)=%d\n", key.ToInteger(), node.entryCnt, len(node.entries))
	return isMadeNewNode
}

func (node *SkipListBlockPage) Remove(key *types.Value, skipPathList []*SkipListBlockPage) (isDeleted bool, level int32) {
	found, _, foundIdx := node.FindEntryByKey(key)
	if found && (node.GetEntryCnt() == 1) {
		// when there are no enry without target entry
		// this node keep reft with no entry (but new entry can be stored)

		if !node.GetEntry(0).Key.CompareEquals(*key) {
			panic("removing wrong entry!")
		}

		// doing connectivity cut here needs accesses to backword nodes
		// and it needs complicated latch (lock) control
		// so, not do connectivity cut here

		//updateLen := int32(mathutil.Min(len(skipPathList), len(node.forward)))
		//
		//// try removing this node from all level of chain
		//// but, some connectivity left often
		//// when several connectivity is left, removing is achieved in later index accesses
		//for ii := int32(0); ii < updateLen; ii++ {
		//	if skipPathList[ii] != nil {
		//		skipPathList[ii].forward[ii] = node.forward[ii]
		//		// mark (ii+1) lebel connectivity is removed
		//		node.forward[ii] = nil
		//	}
		//}

		// this node does not block node traverse in key value compare
		node.GetSmallestKey().SetInfMin()

		node.SetIsNeedDeleted(true)

		return true, node.GetLevel()
	} else if found {
		if !node.GetEntry(foundIdx).Key.CompareEquals(*key) {
			panic("removing wrong entry!")
		}

		formerEntries := make([]SkipListPair, len(node.GetEntries()[:foundIdx]))
		copy(formerEntries, node.GetEntries()[:foundIdx])
		laterEntries := make([]SkipListPair, len(node.GetEntries()[foundIdx+1:]))
		copy(laterEntries, node.GetEntries()[foundIdx+1:])
		formerEntries = append(formerEntries, laterEntries...)
		node.SetEntries(formerEntries)
		//node.entries = append(node.entries[:foundIdx], node.entries[foundIdx+1:]...)
		node.SetSmallestKey(node.GetEntry(0).Key)
		node.SetEntryCnt(int32(len(formerEntries)))
		//node.SetEntryCnt(int32(len(node.GetEntries())))

		return true, node.GetLevel()
	} else { // found == false
		// do nothing
		return false, -1
	}
}

// split entries of node at entry specified with idx arg
// new node contains entries node.entries[idx+1:]
// (new node does not include entry node.entries[idx])
func (node *SkipListBlockPage) SplitNode(idx int32, bpm *buffer.BufferPoolManager, skipPathList []*SkipListBlockPage,
	level int32, curMaxLevel int32, startNode *SkipListBlockPage) {
	//fmt.Println("<<<<<<<<<<<<<<<<<<<<<<<< SplitNode called! >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

	newNode := NewSkipListBlockPage(bpm, level, node.GetEntry(idx+1))
	copyEntries := make([]SkipListPair, len(node.GetEntries()[idx+1:]))
	copy(copyEntries, node.GetEntries()[idx+1:])
	//newNode.entries = append(make([]*SkipListPair, 0), node.entries[idx+1:]...)
	newNode.SetEntries(copyEntries)
	newNode.SetSmallestKey(newNode.GetEntry(0).Key)
	newNode.SetEntryCnt(int32(len(copyEntries)))
	//newNode.SetEntryCnt(int32(len(newNode.GetEntries())))
	newNode.SetLevel(level)
	copyEntriesFormer := make([]SkipListPair, len(node.GetEntries()[:idx+1]))
	copy(copyEntriesFormer, node.GetEntries()[:idx+1])
	node.SetEntries(copyEntriesFormer)
	node.SetSmallestKey(node.GetEntry(0).Key)
	//node.entries = node.entries[:idx+1]
	node.SetEntryCnt(int32(len(node.GetEntries())))

	if level > curMaxLevel {
		skipPathList[level-1] = startNode
	}
	for ii := int32(0); ii < level; ii++ {
		// modify forward link
		newNode.SetForwardEntry(ii, skipPathList[ii].GetForwardEntry(ii))
		skipPathList[ii].SetForwardEntry(ii, newNode.GetPageId())
	}
}

//func (node *SkipListBlockPage) ToDebugString() string {
//	ret := ""
//	ret += fmt.Sprintf("{")
//	// Print smallestKey
//	ret += fmt.Sprintf("%d ", node.GetSmallestKey().ToInteger())
//	// print contents of forward
//	ret += fmt.Sprintf("[")
//	for ii := 0; ii < len(node.GetForward()); ii++ {
//		if node.GetForwardEntry(int32(ii)) == nil {
//			ret += fmt.Sprintf(" nil")
//		} else {
//			ret += fmt.Sprintf(" *")
//		}
//	}
//	ret += fmt.Sprintf("] ")
//	// print IsDeleteNeeded
//	if node.GetIsNeedDeleted() {
//		ret += fmt.Sprintf("true")
//	} else {
//		ret += fmt.Sprintf("false")
//	}
//	ret += fmt.Sprintf("}")
//	return ret
//}

////for debug
//func (node *SkipListBlockPage) CheckCompletelyEmpty() {
//	if !node.GetIsNeedDeleted() {
//		return
//	}
//
//	for ii := 0; ii < len(node.GetForward()); ii++ {
//		if node.GetForwardEntry(int32(ii)) != nil {
//			return
//		}
//	}
//	panic("chekCompletelyEmpty: this node can't be chain!")
//}

func (node *SkipListBlockPage) GetPageId() types.PageID {
	return types.PageID(types.NewUInt32FromBytes(node.Data()[offsetPageId:]))
	//return node.entryCnt
}

func (node *SkipListBlockPage) SetPageId(pageId types.PageID) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, pageId)
	pageIdInBytes := buf.Bytes()
	copy(node.Data()[offsetPageId:], pageIdInBytes)
	//node.entryCnt = cnt
}

func (node *SkipListBlockPage) GetLevel() int32 {
	return int32(types.NewInt32FromBytes(node.Data()[offsetPageId:]))
	//return node.level
}

func (node *SkipListBlockPage) SetLevel(level int32) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, level)
	levelInBytes := buf.Bytes()
	copy(node.Data()[offsetLevel:], levelInBytes)
	//node.level = level
}

func (node *SkipListBlockPage) GetSmallestKey() types.Value {
	//return node.smallestKey
	return node.GetEntry(0).Key
	//return types.NewInteger(-1)
}

func (node *SkipListBlockPage) SetSmallestKey(key types.Value) {
	//node.smallestKey = key
	// TODO: (SDB) debug: do nothing here
}

//func (node *SkipListBlockPage) GetForward() [MAX_FOWARD_LIST_LEN]*SkipListBlockPage {
//	return node.forward
//	//return nil
//}

//func (node *SkipListBlockPage) SetForward(fwd []*SkipListBlockPage) {
//	node.forward = fwd
//}

func (node *SkipListBlockPage) GetForwardEntry(idx int32) types.PageID {
	return types.NewPageIDFromBytes(node.Data()[offsetForward+uint32(idx)*sizeForwardEntry:])
	//return node.forward[idx]
}

func (node *SkipListBlockPage) SetForwardEntry(idx int32, fwdNodeId types.PageID) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, fwdNodeId)
	fwdNodeIdInBytes := buf.Bytes()
	copy(node.Data()[offsetForward+uint32(idx)*sizeForwardEntry:], fwdNodeIdInBytes)
	//node.forward[idx] = fwdNode
}

func (node *SkipListBlockPage) GetEntryCnt() int32 {
	return int32(types.NewInt32FromBytes(node.Data()[offsetEntryCnt:]))
	//return node.entryCnt
}

func (node *SkipListBlockPage) SetEntryCnt(cnt int32) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, cnt)
	cntInBytes := buf.Bytes()
	copy(node.Data()[offsetEntryCnt:], cntInBytes)
	//node.entryCnt = cnt
}

//func (node *SkipListBlockPage) GetMaxEntry() int32 {
//	return node.maxEntry
//	//return -1
//}

//func (node *SkipListBlockPage) SetMaxEntry(num int32) {
//	node.maxEntry = num
//}

func (node *SkipListBlockPage) GetEntries() []SkipListPair {
	return node.entries
	//return nil
}

func (node *SkipListBlockPage) SetEntries(entries []SkipListPair) {
	node.entries = entries
}

func (node *SkipListBlockPage) GetEntry(idx int32) SkipListPair {
	return node.entries[idx]
	//return SkipListPair{types.NewInteger(-1), 0}
}

func (node *SkipListBlockPage) SetEntry(idx int32, entry SkipListPair) {
	node.entries[idx] = entry
}

func (node *SkipListBlockPage) GetIsNeedDeleted() bool {
	return bool(types.NewBoolFromBytes(node.Data()[offsetIsNeedDeleted:]))
	//return node.isNeedDeleted
}

func (node *SkipListBlockPage) SetIsNeedDeleted(val bool) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, val)
	valInBytes := buf.Bytes()
	copy(node.Data()[offsetIsNeedDeleted:], valInBytes)
	//node.isNeedDeleted = val
}

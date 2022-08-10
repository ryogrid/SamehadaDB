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
//  | Entry_0 offset (4) | Entry_0 size (4) | ..................|
//  ------------------------------------------------------------
//  ^ offsetEntryInfos (=sizeBlockPageHeaderExceptEntryInfos)
//
//  Entries format (size in bytes):
//  ----------------------------------------------------------------------------------------------------------------------------------
//  | HEADER | ... FREE SPACE ... | ....more entries....| Entry1_key (1+x) | Entry_1 value (4) | Entry0_key (1+x) | Entry_0 value (4)|
//  ---------------------------------------------------------------------------------------------------------------------------------
//                                ^                    ^ <-------------- size ---------------> ^ <-------------- size --------------->
//                                freeSpacePointer     offset(from page head)                  offset(...)
//
//  Entry_key format (size in bytes)
//    = Serialized types.Value
//  ----------------------------------------------
//  | isNull (1) | data according to KeyType (x) |
//  ----------------------------------------------

const (
	// TODO: (SDB) need to modify codes referencing DUMMY_MAX_ENTRY for on disk support
	//             above means implemantation of free space amount check at Insert entry at least
	DUMMY_MAX_ENTRY                     = 100 //10 //50
	sizePageId                          = uint32(4)
	sizeLevel                           = uint32(4)
	sizeEntryCnt                        = uint32(4)
	sizeIsNeedDeleted                   = uint32(1)
	sizeForward                         = uint32(4 * MAX_FOWARD_LIST_LEN)
	sizeForwardEntry                    = uint32(4) // types.PageID
	sizeFreeSpacePointer                = uint32(4)
	sizeEntryInfoOffset                 = uint32(4)
	sizeEntryInfoSize                   = uint32(4)
	sizeEntryInfo                       = sizeEntryInfoOffset + sizeEntryInfoSize
	sizeBlockPageHeaderExceptEntryInfos = sizePageId + sizeLevel + sizeEntryCnt + sizeIsNeedDeleted + sizeForward + sizeFreeSpacePointer
	offsetPageId                        = int32(0)
	offsetLevel                         = sizePageId
	offsetEntryCnt                      = offsetLevel + sizeLevel
	offsetIsNeedDeleted                 = offsetEntryCnt + sizeEntryCnt
	offsetForward                       = offsetIsNeedDeleted + sizeIsNeedDeleted
	offsetFreeSpacePointer              = offsetForward + sizeForward
	offsetEntryInfos                    = offsetFreeSpacePointer + sizeFreeSpacePointer
	sizeEntryValue                      = uint32(4)
)

type SkipListPair struct {
	Key   types.Value
	Value uint32
}

func (sp SkipListPair) Serialize() []byte {
	keyInBytes := sp.Key.Serialize()
	valBuf := new(bytes.Buffer)
	binary.Write(valBuf, binary.LittleEndian, sp.Value)
	valInBytes := valBuf.Bytes()

	retBuf := new(bytes.Buffer)
	retBuf.Write(keyInBytes)
	retBuf.Write(valInBytes)
	return retBuf.Bytes()
}

func NewSkipListPairFromBytes(buf []byte, keyType types.TypeID) *SkipListPair {
	dataLen := len(buf)
	valPartOffset := dataLen - int(sizeEntryValue)
	key := types.NewValueFromBytes(buf[:valPartOffset], keyType)
	value := uint32(types.NewUInt32FromBytes(buf[valPartOffset:]))
	return &SkipListPair{*key, value}
}

func (sp SkipListPair) GetDataSize() uint32 {
	keyInBytes := sp.Key.Serialize()

	return uint32(len(keyInBytes) + 4)
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

// TODO: (SDB) caller must call UnpinPage with appropriate dirty flag call to returned object
func NewSkipListBlockPage(bpm *buffer.BufferPoolManager, level int32, smallestListPair SkipListPair) *SkipListBlockPage {
	page_ := bpm.NewPage()
	if page_ == nil {
		panic("NewPage can't allocate more page!")
		//return nil
	}

	//ret := new(SkipListBlockPage)
	ret := (*SkipListBlockPage)(unsafe.Pointer(page_))
	ret.SetPageId(page_.ID())
	ret.SetEntryCnt(0)
	ret.SetLevel(level)
	ret.SetIsNeedDeleted(false)
	ret.initForwardEntries()
	ret.SetFreeSpacePointer(common.PageSize)
	//ret.SetEntries(make([]*SkipListPair, 0)) // for first insert works
	tmpSmallestListPair := smallestListPair
	// EntryCnt is incremented to 1
	ret.SetEntry(0, &tmpSmallestListPair)
	//ret.SetEntries(append(ret.GetEntries(smallestListPair.Key.ValueType()), &tmpSmallestListPair))
	//ret.SetSmallestKey(smallestListPair.Key)

	//ret.SetMaxEntry(DUMMY_MAX_ENTRY)
	//ret.SetForward(make([]*SkipListBlockPage, level))
	//ret.Backward = make([]*SkipListBlockPage, level)

	return ret
}

func (node *SkipListBlockPage) initForwardEntries() {
	for ii := 0; ii < MAX_FOWARD_LIST_LEN; ii++ {
		node.SetForwardEntry(ii, common.InvalidPageID)
	}
}

//// Gets the entry at index in this node
//func (node *SkipListBlockPage) EntryAt(idx int32, keyType types.TypeID) *SkipListPair {
//	return node.GetEntry(int(idx), keyType)
//}

// Gets the key at index in this node
func (node *SkipListBlockPage) KeyAt(idx int32, keyType types.TypeID) *types.Value {
	key := node.GetEntry(int(idx), keyType).Key
	return &key
}

// Gets the value at an index in this node
func (node *SkipListBlockPage) ValueAt(idx int32, keyType types.TypeID) uint32 {
	val := node.GetEntry(int(idx), keyType).Value
	return val
}

// if not found, returns info of nearest smaller key
// binary search is used for search
// https://www.cs.usfca.edu/~galles/visualization/Search.html
func (node *SkipListBlockPage) FindEntryByKey(key *types.Value) (found bool, entry *SkipListPair, index int32) {
	if node.GetEntryCnt() == 1 {
		if node.GetEntry(0, key.ValueType()).Key.CompareEquals(*key) {
			return true, node.GetEntry(0, key.ValueType()), 0
		} else {
			if node.GetEntry(0, key.ValueType()).Key.IsInfMin() {
				return false, node.GetEntry(0, key.ValueType()), 0
			} else {
				if node.GetEntry(0, key.ValueType()).Key.CompareLessThan(*key) {
					return false, node.GetEntry(0, key.ValueType()), 0
				} else {
					return false, node.GetEntry(0, key.ValueType()), -1
				}

			}
		}
	} else {
		lowIdx := int32(0)
		highIdx := node.GetEntryCnt() - 1
		midIdx := int32(-1)

		for lowIdx <= highIdx {
			midIdx = (lowIdx + highIdx) / 2
			if node.KeyAt(midIdx, key.ValueType()).CompareEquals(*key) {
				return true, node.GetEntry(int(midIdx), key.ValueType()), midIdx
			} else if node.KeyAt(midIdx, key.ValueType()).CompareLessThan(*key) {
				lowIdx = midIdx + 1
			} else {
				highIdx = midIdx - 1
			}
		}
		if lowIdx < highIdx {
			return false, node.GetEntry(int(lowIdx), key.ValueType()), lowIdx
		} else {
			if highIdx < 0 {
				return false, node.GetEntry(0, key.ValueType()), 0
			} else {
				return false, node.GetEntry(int(highIdx), key.ValueType()), highIdx
			}
		}
	}

}

// Attempts to insert a key and value into an index in the baccess
// return value is whether newNode is created or not
func (node *SkipListBlockPage) Insert(key *types.Value, value uint32, bpm *buffer.BufferPoolManager, skipPathList []types.PageID,
	level int32, curMaxLevel int32, startNode *SkipListBlockPage) bool {
	//fmt.Printf("Insert of SkipListBlockPage called! : key=%d\n", key.ToInteger())

	found, _, foundIdx := node.FindEntryByKey(key)
	isMadeNewNode := false
	var splitIdx int32 = -1
	if found {
		//fmt.Println("found at Insert")
		// over write exsiting entry
		if !node.GetEntry(int(foundIdx), key.ValueType()).Key.CompareEquals(*key) {
			panic("overwriting wrong entry!")
		}

		if node.GetEntry(int(foundIdx), key.ValueType()).Key.CompareEquals(*key) {
			panic("key duplication is not supported yet!")
		}

		node.SetEntry(int(foundIdx), &SkipListPair{*key, value})
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
			skipPathList[0] = node.GetPageId()
			node.SplitNode(splitIdx, bpm, skipPathList, level, curMaxLevel, startNode, key.ValueType())
			isMadeNewNode = true

			if foundIdx > splitIdx {
				// insert to new node
				newSmallerIdx := foundIdx - splitIdx - 1
				newNodePageId := node.GetForwardEntry(0)
				//page_ := bpm.FetchPage(newNodePageId)
				//newNode := (*SkipListBlockPage)(unsafe.Pointer(page_))
				newNode := FetchAndCastToBlockPage(bpm, newNodePageId)

				if (newSmallerIdx + 1) >= newNode.GetEntryCnt() {
					// when inserting point is next of last entry of new node
					common.SH_Assert(newNode.GetEntry(len(newNode.GetEntries(key.ValueType()))-1, key.ValueType()).Key.CompareLessThan(*key), "order is invalid.")
					copiedEntries := make([]*SkipListPair, len(newNode.GetEntries(key.ValueType())))
					copy(copiedEntries, newNode.GetEntries(key.ValueType())[:])
					newNode.SetEntries(append(copiedEntries, &SkipListPair{*key, value}))
					//newNode.entries = append(newNode.entries, &SkipListPair{*key, value})
				} else {
					formerEntries := make([]*SkipListPair, len(newNode.GetEntries(key.ValueType())[:newSmallerIdx+1]))
					copy(formerEntries, newNode.GetEntries(key.ValueType())[:newSmallerIdx+1])
					laterEntries := make([]*SkipListPair, len(newNode.GetEntries(key.ValueType())[newSmallerIdx+1:]))
					copy(laterEntries, newNode.GetEntries(key.ValueType())[newSmallerIdx+1:])
					formerEntries = append(formerEntries, &SkipListPair{*key, value})
					formerEntries = append(formerEntries, laterEntries...)
					newNode.SetEntries(formerEntries)
				}
				newNode.SetSmallestKey(newNode.GetEntry(0, key.ValueType()).Key)
				newNode.SetEntryCnt(int32(len(newNode.GetEntries(key.ValueType()))))

				bpm.UnpinPage(newNode.GetPageId(), true)
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
			common.SH_Assert(node.GetEntry(len(node.GetEntries(key.ValueType()))-1, key.ValueType()).Key.IsInfMin() || node.GetEntry(len(node.GetEntries(key.ValueType()))-1, key.ValueType()).Key.CompareLessThan(*key), "order is invalid.")
			copiedEntries := make([]*SkipListPair, len(node.GetEntries(key.ValueType())))
			copy(copiedEntries, node.GetEntries(key.ValueType())[:])
			node.SetEntries(append(copiedEntries, &SkipListPair{*key, value}))
			toSetEntryCnt = int32(len(copiedEntries) + 1)
			//node.entries = append(node.entries, SkipListPair{*key, value})
		} else {
			if foundIdx == -1 {
				var copiedEntries []*SkipListPair = nil
				if isMadeNewNode {
					copiedEntries = make([]*SkipListPair, len(node.GetEntries(key.ValueType())[:splitIdx+1]))
					copy(copiedEntries, node.GetEntries(key.ValueType())[:splitIdx+1])
				} else {
					copiedEntries = make([]*SkipListPair, len(node.GetEntries(key.ValueType())[:]))
					copy(copiedEntries, node.GetEntries(key.ValueType())[:])
				}
				finalEntriles := make([]*SkipListPair, 0)
				finalEntriles = append(finalEntriles, &SkipListPair{*key, value})
				node.SetEntries(append(finalEntriles, copiedEntries...))
				toSetEntryCnt = int32(len(finalEntriles) + len(copiedEntries))
			} else {
				formerEntries := make([]*SkipListPair, len(node.GetEntries(key.ValueType())[:foundIdx+1]))
				copy(formerEntries, node.GetEntries(key.ValueType())[:foundIdx+1])
				var laterEntries []*SkipListPair = nil
				if isMadeNewNode {
					laterEntries = make([]*SkipListPair, len(node.GetEntries(key.ValueType())[foundIdx+1:splitIdx+1]))
					copy(laterEntries, node.GetEntries(key.ValueType())[foundIdx+1:splitIdx+1])
				} else {
					laterEntries = make([]*SkipListPair, len(node.GetEntries(key.ValueType())[foundIdx+1:]))
					copy(laterEntries, node.GetEntries(key.ValueType())[foundIdx+1:])
				}

				formerEntries = append(formerEntries, &SkipListPair{*key, value})
				formerEntries = append(formerEntries, laterEntries...)
				node.SetEntries(formerEntries)
				toSetEntryCnt = int32(len(formerEntries))
			}
		}
		node.SetSmallestKey(node.GetEntry(0, key.ValueType()).Key)
		//node.SetEntryCnt(int32(len(node.GetEntries(key.ValueType()))))
		node.SetEntryCnt(toSetEntryCnt)
	}
	//fmt.Printf("end of Insert of SkipListBlockPage called! : key=%d page.entryCnt=%d len(page.entries)=%d\n", key.ToInteger(), node.entryCnt, len(node.entries))
	return isMadeNewNode
}

func (node *SkipListBlockPage) Remove(key *types.Value, skipPathList []types.PageID) (isDeleted bool, level int32) {
	found, _, foundIdx := node.FindEntryByKey(key)
	if found && (node.GetEntryCnt() == 1) {
		// when there are no enry without target entry
		// this node keep reft with no entry (but new entry can be stored)

		if !node.GetEntry(0, key.ValueType()).Key.CompareEquals(*key) {
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
		tmpEntries := make([]*SkipListPair, 0)
		tmpEntries = append(tmpEntries, &SkipListPair{*node.GetSmallestKey(key.ValueType()).SetInfMin(), 0})
		node.SetEntries(tmpEntries)

		node.SetIsNeedDeleted(true)

		return true, node.GetLevel()
	} else if found {
		if !node.GetEntry(int(foundIdx), key.ValueType()).Key.CompareEquals(*key) {
			panic("removing wrong entry!")
		}

		formerEntries := make([]*SkipListPair, len(node.GetEntries(key.ValueType())[:foundIdx]))
		copy(formerEntries, node.GetEntries(key.ValueType())[:foundIdx])
		laterEntries := make([]*SkipListPair, len(node.GetEntries(key.ValueType())[foundIdx+1:]))
		copy(laterEntries, node.GetEntries(key.ValueType())[foundIdx+1:])
		formerEntries = append(formerEntries, laterEntries...)
		node.SetEntries(formerEntries)
		//node.entries = append(node.entries[:foundIdx], node.entries[foundIdx+1:]...)
		node.SetSmallestKey(node.GetEntry(0, key.ValueType()).Key)
		node.SetEntryCnt(int32(len(formerEntries)))
		//node.SetEntryCnt(int32(len(node.GetEntries(key.ValueType()))))

		return true, node.GetLevel()
	} else { // found == false
		// do nothing
		return false, -1
	}
}

// split entries of node at entry specified with idx arg
// new node contains entries node.entries[idx+1:]
// (new node does not include entry node.entries[idx])
func (node *SkipListBlockPage) SplitNode(idx int32, bpm *buffer.BufferPoolManager, skipPathList []types.PageID,
	level int32, curMaxLevel int32, startNode *SkipListBlockPage, keyType types.TypeID) {
	//fmt.Println("<<<<<<<<<<<<<<<<<<<<<<<< SplitNode called! >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

	newNode := NewSkipListBlockPage(bpm, level, *node.GetEntry(int(idx+1), keyType))
	copyEntries := make([]*SkipListPair, len(node.GetEntries(keyType)[idx+1:]))
	copy(copyEntries, node.GetEntries(keyType)[idx+1:])
	//newNode.entries = append(make([]*SkipListPair, 0), node.entries[idx+1:]...)
	newNode.SetEntries(copyEntries)
	newNode.SetSmallestKey(newNode.GetEntry(0, keyType).Key)
	newNode.SetEntryCnt(int32(len(copyEntries)))
	//newNode.SetEntryCnt(int32(len(newNode.GetEntries(keyType))))
	newNode.SetLevel(level)
	copyEntriesFormer := make([]*SkipListPair, len(node.GetEntries(keyType)[:idx+1]))
	copy(copyEntriesFormer, node.GetEntries(keyType)[:idx+1])
	node.SetEntries(copyEntriesFormer)
	node.SetSmallestKey(node.GetEntry(0, keyType).Key)
	//node.entries = node.entries[:idx+1]
	node.SetEntryCnt(int32(len(node.GetEntries(keyType))))

	if level > curMaxLevel {
		skipPathList[level-1] = startNode.GetPageId()
	}
	for ii := 0; ii < int(level); ii++ {
		// modify forward link
		tmpNode := FetchAndCastToBlockPage(bpm, skipPathList[ii])
		newNode.SetForwardEntry(ii, tmpNode.GetForwardEntry(ii))
		tmpNode.SetForwardEntry(ii, newNode.GetPageId())
		bpm.UnpinPage(tmpNode.GetPageId(), true)
	}
	bpm.UnpinPage(newNode.GetPageId(), true)
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
	return int32(types.NewInt32FromBytes(node.Data()[offsetLevel:]))
	//return node.level
}

func (node *SkipListBlockPage) SetLevel(level int32) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, level)
	levelInBytes := buf.Bytes()
	copy(node.Data()[offsetLevel:], levelInBytes)
	//node.level = level
}

func (node *SkipListBlockPage) GetSmallestKey(keyType types.TypeID) types.Value {
	//return node.smallestKey
	return node.GetEntry(0, keyType).Key
	//return types.NewInteger(-1)
}

func (node *SkipListBlockPage) SetSmallestKey(key types.Value) {
	//node.smallestKey = key
	// TODO: (SDB) this method and calling of this method should be removed (SetSmallestKey)
}

//func (node *SkipListBlockPage) GetForward() [MAX_FOWARD_LIST_LEN]*SkipListBlockPage {
//	return node.forward
//	//return nil
//}

//func (node *SkipListBlockPage) SetForward(fwd []*SkipListBlockPage) {
//	node.forward = fwd
//}

func (node *SkipListBlockPage) GetForwardEntry(idx int) types.PageID {
	return types.NewPageIDFromBytes(node.Data()[offsetForward+uint32(idx)*sizeForwardEntry:])
	//return node.forward[idx]
}

func (node *SkipListBlockPage) SetForwardEntry(idx int, fwdNodeId types.PageID) {
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

func (node *SkipListBlockPage) GetEntries(keyType types.TypeID) []*SkipListPair {
	entryNum := int(node.GetEntryCnt())
	retArr := make([]*SkipListPair, 0)
	for ii := 0; ii < entryNum; ii++ {
		retArr = append(retArr, node.GetEntry(ii, keyType))
	}
	return retArr
	//return node.entries
}

func (node *SkipListBlockPage) GetEntryOffset(idx int) uint32 {
	offset := offsetEntryInfos + sizeEntryInfo*uint32(idx)

	return uint32(types.NewUInt32FromBytes(node.Data()[offset : offset+sizeEntryInfoOffset]))
}

func (node *SkipListBlockPage) SetEntryOffset(idx int, setOffset uint32) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, setOffset)
	setOffsetInBytes := buf.Bytes()
	offset := offsetEntryInfos + sizeEntryInfo*uint32(idx)
	copy(node.Data()[offset:], setOffsetInBytes)
}

func (node *SkipListBlockPage) GetEntrySize(idx int) uint32 {
	offset := offsetEntryInfos + sizeEntryInfo*uint32(idx) + sizeEntryInfoOffset

	return uint32(types.NewUInt32FromBytes(node.Data()[offset : offset+sizeEntryInfoSize]))
}

func (node *SkipListBlockPage) SetEntrySize(idx int, setSize uint32) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, setSize)
	setSizeInBytes := buf.Bytes()
	offset := offsetEntryInfos + sizeEntryInfo*uint32(idx) + sizeEntryInfoOffset
	copy(node.Data()[offset:], setSizeInBytes)
}

// memo: freeSpacePointer value is index of buffer which points already data placed
//       so, you can use memory Data()[someOffset:freeSpacePointer] in golang description
func (node *SkipListBlockPage) GetFreeSpacePointer() uint32 {
	offset := offsetFreeSpacePointer

	return uint32(types.NewUInt32FromBytes(node.Data()[offset : offset+sizeFreeSpacePointer]))
}

func (node *SkipListBlockPage) SetFreeSpacePointer(pointOffset uint32) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, pointOffset)
	pointOffsetInBytes := buf.Bytes()
	offset := offsetFreeSpacePointer
	copy(node.Data()[offset:], pointOffsetInBytes)
}

func (node *SkipListBlockPage) GetEntry(idx int, keyType types.TypeID) *SkipListPair {
	offset := node.GetEntryOffset(idx)
	return NewSkipListPairFromBytes(node.Data()[offset:offset+node.GetEntrySize(idx)], keyType)
	//return node.entries[idx]
}

// ATTENTION:
// this method can be called only when...
//  - it is guranteed that new entry insert doesn't cause overflow of node space capacity
//  - key of target entry doesn't exist in this node
func (node *SkipListBlockPage) SetEntry(idx int, entry *SkipListPair) {
	// at current design,
	// - duplicated key is not supported
	// - this SkipList is used for index of RDBMS, so update of value part (stores record data offset at DB file)
	//   doesn't occur on thisl method call (it is guranteed by caller)
	//     => - it is guranteed that key of entry doesn't exist in this node
	//     => - passed data can be placed to tail of entry data area (address is specified by freeSpacePointer)
	appendData := entry.Serialize()
	entrySize := uint32(len(appendData))
	newFSP := node.GetFreeSpacePointer() - entrySize
	copy(node.Data()[newFSP:], appendData)
	node.SetFreeSpacePointer(newFSP)
	node.SetEntryOffset(idx, newFSP)
	node.SetEntrySize(idx, entrySize)
	node.SetEntryCnt(node.GetEntryCnt() + 1)
}

func (node *SkipListBlockPage) SetEntries(entries []*SkipListPair) {
	buf := new(bytes.Buffer)
	entryNum := len(entries)
	// order of elements on buf becomes descending order in contrast with entries arg

	entrySizes := make([]uint32, entryNum)
	for ii := entryNum - 1; ii >= 0; ii-- {
		entry := entries[ii]
		entryData := entry.Serialize()
		buf.Write(entryData)

		// update each entry location info in header
		entrySize := len(entryData)
		node.SetEntrySize(ii, uint32(entrySize))
		entrySizes[ii] = uint32(entrySize)
	}
	entriesInBytes := buf.Bytes()
	copySize := len(entriesInBytes)
	offset := common.PageSize - copySize
	copy(node.Data()[offset:], entriesInBytes)

	// set entry offset infos here
	// because offset info can't calculate above loop efficiently
	entryOffset := uint32(common.PageSize)
	for ii := 0; ii < entryNum; ii++ {
		entryOffset = entryOffset - entrySizes[ii]
		node.SetEntryOffset(ii, entryOffset)
	}

	// update entries info in header
	node.SetFreeSpacePointer(uint32(offset))
	node.SetEntryCnt(int32(entryNum))
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

// since header space grow with insertion entry, memory size which is needed
// for insertion is not same with size of SkipListPair object
func (node *SkipListBlockPage) GetSpecifiedSLPNeedSpace(slp *SkipListPair) uint32 {
	return slp.GetDataSize() + sizeEntryInfo
}

// returns remaining bytes for additional entry
func (node *SkipListBlockPage) getFreeSpaceRemaining() uint32 {
	return (node.GetFreeSpacePointer() - 1) - ((offsetEntryInfos + (sizeEntryInfo * uint32(node.GetEntryCnt()))) - 1)
}

// TODO: (SDB) in concurrent impl, locking in this method is needed. and caller must do unlock (FectchAndCastToBlockPage)

// Attention:
//   caller must call UnpinPage with appropriate diaty page to the got page when page using ends
func FetchAndCastToBlockPage(bpm *buffer.BufferPoolManager, pageId types.PageID) *SkipListBlockPage {
	bPage := bpm.FetchPage(pageId)
	return (*SkipListBlockPage)(unsafe.Pointer(bPage))
}

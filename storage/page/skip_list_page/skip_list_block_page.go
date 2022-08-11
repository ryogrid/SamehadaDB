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
//  | Entry_0 offset (2) | Entry_0 size (2) | ..................|
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
// Note:
//  placement order of entry location data on header doesn't match with entry data on payload
//  because entry insertion cost is keeped lower
//
//  Entry_key format (size in bytes)
//    = Serialized types.Value
//  ----------------------------------------------
//  | isNull (1) | data according to KeyType (x) |
//  ----------------------------------------------

const (
	sizePageId                          = uint32(4)
	sizeLevel                           = uint32(4)
	sizeEntryCnt                        = uint32(4)
	sizeIsNeedDeleted                   = uint32(1)
	sizeForward                         = uint32(4 * MAX_FOWARD_LIST_LEN)
	sizeForwardEntry                    = uint32(4) // types.PageID
	sizeFreeSpacePointer                = uint32(4)
	sizeEntryInfoOffset                 = uint32(2)
	sizeEntryInfoSize                   = uint32(2)
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

	return uint32(len(keyInBytes)) + sizeEntryValue
}

type SkipListBlockPage struct {
	page.Page
}

// ATTENTION: caller must call UnpinPage with appropriate dirty flag call to returned object
func NewSkipListBlockPage(bpm *buffer.BufferPoolManager, level int32, smallestListPair SkipListPair) *SkipListBlockPage {
	page_ := bpm.NewPage()
	if page_ == nil {
		panic("NewPage can't allocate more page!")
		//return nil
	}

	ret := (*SkipListBlockPage)(unsafe.Pointer(page_))
	ret.SetPageId(page_.ID())
	ret.SetEntryCnt(0)
	ret.SetLevel(level)
	ret.SetIsNeedDeleted(false)
	ret.initForwardEntries()
	ret.SetFreeSpacePointer(common.PageSize)
	tmpSmallestListPair := smallestListPair
	// EntryCnt is incremented to 1
	ret.SetEntry(0, &tmpSmallestListPair)

	return ret
}

func (node *SkipListBlockPage) initForwardEntries() {
	for ii := 0; ii < MAX_FOWARD_LIST_LEN; ii++ {
		node.SetForwardEntry(ii, common.InvalidPageID)
	}
}

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

// insert entry locatin info to page header next of idx index entry
// this method call slides memory area of header using memory copy
// idx==-1 -> data's inddx become 0 (insert to head of entries)
// idx==entryCnt -> data's index become entryCnt (insert next of last entry)
// ATTENTION:
//   caller should update entryCnt appropriatery after this method call
func (node *SkipListBlockPage) updateEntryInfosAtInsert(idx int, dataSize uint16) {
	// entrries data backward of entry which specifed with idx arg are not changed
	// because data of new entry is always placed tail of payload area
	// and so, this method needs offset info of new entry on arg

	// entrries info data backward of entry which specifed with idx arg is slided for
	// new entry insertion
	allEntryNum := uint32(node.GetEntryCnt())
	slideFromOffset := offsetEntryInfos + uint32(idx+1)*sizeEntryInfo
	slideToOffset := offsetEntryInfos + uint32(idx+2)*sizeEntryInfo
	slideAreaStartOffset := slideFromOffset
	slideAreaEndOffset := offsetEntryInfos + allEntryNum*sizeEntryInfo
	copy(node.Data()[slideToOffset:], node.Data()[slideAreaStartOffset:slideAreaEndOffset])

	// set data of new entry
	entryOffsetVal := node.GetFreeSpacePointer()
	node.SetEntryOffset(idx+1, uint16(entryOffsetVal))
	node.SetEntrySize(idx+1, dataSize)
}

// insert serialized data of slp arg next of idx index entry
// this method slides memory area of header using memory copy
// in contrast, new entry data is placed always tail of entries data area
// idx==-1 -> data's inddx become 0 (insert to head of entries)
// idx==entryCnt -> data's index become entryCnt (insert next of last entry)
func (node *SkipListBlockPage) InsertInner(idx int, slp *SkipListPair) {
	// data copy of slp arg to tail of entry data space
	// the tail is pointed by freeSpacePointer
	insertData := slp.Serialize()
	insertEntrySize := uint32(len(insertData))
	offset := node.GetFreeSpacePointer() - insertEntrySize
	copy(node.Data()[offset:], insertData)
	node.SetFreeSpacePointer(offset)

	node.updateEntryInfosAtInsert(idx, uint16(insertEntrySize))
	node.SetEntryCnt(node.GetEntryCnt() + 1)
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
		if node.getFreeSpaceRemaining() < node.GetSpecifiedSLPNeedSpace(&SkipListPair{*key, value}) {
			// this node is full. so node split is needed

			// first, split this node at center of entry list
			// half of entries are moved to new node
			splitIdx = node.GetEntryCnt() / 2
			// update with this node
			skipPathList[0] = node.GetPageId()
			node.SplitNode(splitIdx, bpm, skipPathList, level, curMaxLevel, startNode, key.ValueType())
			isMadeNewNode = true

			if foundIdx > splitIdx {
				// insert to new node
				newSmallerIdx := foundIdx - splitIdx - 1
				newNodePageId := node.GetForwardEntry(0)
				newNode := FetchAndCastToBlockPage(bpm, newNodePageId)

				if (newSmallerIdx + 1) >= newNode.GetEntryCnt() {
					// when inserting point is next of last entry of new node
					//common.SH_Assert(newNode.GetEntry(len(newNode.GetEntries(key.ValueType()))-1, key.ValueType()).Key.CompareLessThan(*key), "order is invalid.")

					//copiedEntries := make([]*SkipListPair, len(newNode.GetEntries(key.ValueType())))
					//copy(copiedEntries, newNode.GetEntries(key.ValueType())[:])
					//newNode.SetEntries(append(copiedEntries, &SkipListPair{*key, value}))
					newNode.InsertInner(int(newSmallerIdx), &SkipListPair{*key, value})
				} else {
					//formerEntries := make([]*SkipListPair, len(newNode.GetEntries(key.ValueType())[:newSmallerIdx+1]))
					//copy(formerEntries, newNode.GetEntries(key.ValueType())[:newSmallerIdx+1])
					//laterEntries := make([]*SkipListPair, len(newNode.GetEntries(key.ValueType())[newSmallerIdx+1:]))
					//copy(laterEntries, newNode.GetEntries(key.ValueType())[newSmallerIdx+1:])
					//formerEntries = append(formerEntries, &SkipListPair{*key, value})
					//formerEntries = append(formerEntries, laterEntries...)
					//newNode.SetEntries(formerEntries)
					newNode.InsertInner(int(newSmallerIdx), &SkipListPair{*key, value})
				}
				//newNode.SetEntryCnt(int32(len(newNode.GetEntries(key.ValueType()))))

				bpm.UnpinPage(newNode.GetPageId(), true)
				//fmt.Printf("end of Insert of SkipListBlockPage called! : key=%d page.entryCnt=%d len(page.entries)=%d\n", key.ToInteger(), node.entryCnt, len(node.entries))

				return isMadeNewNode
			} // else => insert to this node
		}
		// insert to this node
		// foundIdx is index of nearlest smaller key entry
		// new entry is inserted next of nearlest smaller key entry

		//var toSetEntryCnt int32 = -1
		if (foundIdx + 1) >= node.GetEntryCnt() {
			// when inserting point is next of last entry of this node
			//common.SH_Assert(node.GetEntry(len(node.GetEntries(key.ValueType()))-1, key.ValueType()).Key.IsInfMin() || node.GetEntry(len(node.GetEntries(key.ValueType()))-1, key.ValueType()).Key.CompareLessThan(*key), "order is invalid.")

			//copiedEntries := make([]*SkipListPair, len(node.GetEntries(key.ValueType())))
			//copy(copiedEntries, node.GetEntries(key.ValueType())[:])
			//node.SetEntries(append(copiedEntries, &SkipListPair{*key, value}))
			//toSetEntryCnt = int32(len(copiedEntries) + 1)
			node.InsertInner(int(foundIdx), &SkipListPair{*key, value})
		} else {
			if foundIdx == -1 {
				//var copiedEntries []*SkipListPair = nil
				if isMadeNewNode {
					//copiedEntries = make([]*SkipListPair, len(node.GetEntries(key.ValueType())[:splitIdx+1]))
					//copy(copiedEntries, node.GetEntries(key.ValueType())[:splitIdx+1])
					node.InsertInner(-1, &SkipListPair{*key, value})
				} else {
					//copiedEntries = make([]*SkipListPair, len(node.GetEntries(key.ValueType())[:]))
					//copy(copiedEntries, node.GetEntries(key.ValueType())[:])
					node.InsertInner(-1, &SkipListPair{*key, value})
				}
				//finalEntriles := make([]*SkipListPair, 0)
				//finalEntriles = append(finalEntriles, &SkipListPair{*key, value})
				//node.SetEntries(append(finalEntriles, copiedEntries...))
				//toSetEntryCnt = int32(len(finalEntriles) + len(copiedEntries))
			} else {
				//formerEntries := make([]*SkipListPair, len(node.GetEntries(key.ValueType())[:foundIdx+1]))
				//copy(formerEntries, node.GetEntries(key.ValueType())[:foundIdx+1])
				//var laterEntries []*SkipListPair = nil
				if isMadeNewNode {
					//laterEntries = make([]*SkipListPair, len(node.GetEntries(key.ValueType())[foundIdx+1:splitIdx+1]))
					//copy(laterEntries, node.GetEntries(key.ValueType())[foundIdx+1:splitIdx+1])
					node.InsertInner(int(foundIdx), &SkipListPair{*key, value})
				} else {
					//laterEntries = make([]*SkipListPair, len(node.GetEntries(key.ValueType())[foundIdx+1:]))
					//copy(laterEntries, node.GetEntries(key.ValueType())[foundIdx+1:])
					node.InsertInner(int(foundIdx), &SkipListPair{*key, value})
				}

				//formerEntries = append(formerEntries, &SkipListPair{*key, value})
				//formerEntries = append(formerEntries, laterEntries...)
				//node.SetEntries(formerEntries)
				//toSetEntryCnt = int32(len(formerEntries))
			}
		}
		//node.SetEntryCnt(toSetEntryCnt)
	}
	//fmt.Printf("end of Insert of SkipListBlockPage called! : key=%d page.entryCnt=%d len(page.entries)=%d\n", key.ToInteger(), node.entryCnt, len(node.entries))
	return isMadeNewNode
}

// remove entry info specified with idx arg from header of page
// this method slides memory area of header using memory copy
// ATTENTION:
//   caller should update entryCnt appropriatery after this method call
func (node *SkipListBlockPage) updateEntryInfosAtRemove(idx int) {
	dataSize := node.GetEntrySize(idx)
	orgDataOffset := node.GetEntryOffset(idx)
	allEntryNum := uint32(node.GetEntryCnt())

	// entries info data backward of entry which specifed with idx arg needs to be updated
	for ii := 0; ii < int(allEntryNum); ii++ {
		if offset := node.GetEntryOffset(ii); offset < orgDataOffset {
			node.SetEntryOffset(ii, offset+dataSize)
		}
	}

	// entrries info data backward of entry which specifed with idx arg is slided for working
	// out partial free space
	slideToOffset := offsetEntryInfos + uint32(idx)*sizeEntryInfo
	slideAreaStartOffset := slideToOffset + sizeEntryInfo
	slideAreaEndOffset := offsetEntryInfos + allEntryNum*sizeEntryInfo
	copy(node.Data()[slideToOffset:], node.Data()[slideAreaStartOffset:slideAreaEndOffset])
}

// remove entry index specified with idx arg
// this method slides memory area of header and payload using memory copy
func (node *SkipListBlockPage) RemoveInner(idx int) {
	// entrries data backward of entry which specifed with idx arg is slided for working
	// out partial free space
	removeEntrySize := node.GetEntrySize(idx)
	removeEntryOffset := node.GetEntryOffset(idx)
	slideAreaStartOffset := node.GetFreeSpacePointer()
	slideAreaEndOffset := removeEntryOffset
	slideToStartOffset := slideAreaStartOffset + uint32(removeEntrySize)
	//slideToEndOffset := removeEntryOffset + removeEntrySize
	copy(node.Data()[slideToStartOffset:], node.Data()[slideAreaStartOffset:slideAreaEndOffset])
	node.SetFreeSpacePointer(slideToStartOffset)

	node.updateEntryInfosAtRemove(idx)

	node.SetEntryCnt(node.GetEntryCnt() - 1)
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

		//formerEntries := make([]*SkipListPair, len(node.GetEntries(key.ValueType())[:foundIdx]))
		//copy(formerEntries, node.GetEntries(key.ValueType())[:foundIdx])
		//laterEntries := make([]*SkipListPair, len(node.GetEntries(key.ValueType())[foundIdx+1:]))
		//copy(laterEntries, node.GetEntries(key.ValueType())[foundIdx+1:])
		//formerEntries = append(formerEntries, laterEntries...)
		//node.SetEntries(formerEntries)
		//node.SetEntryCnt(int32(len(formerEntries)))
		node.RemoveInner(int(foundIdx))

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
	//copyEntries := make([]*SkipListPair, len(node.GetEntries(keyType)[idx+1:]))
	//copy(copyEntries, node.GetEntries(keyType)[idx+1:])
	//newNode.SetEntries(copyEntries)
	//newNode.SetEntryCnt(int32(len(copyEntries)))
	newNode.SetEntries(node.GetEntries(keyType)[idx+1:])
	newNode.SetLevel(level)
	//copyEntriesFormer := make([]*SkipListPair, len(node.GetEntries(keyType)[:idx+1]))
	//copy(copyEntriesFormer, node.GetEntries(keyType)[:idx+1])
	//node.SetEntries(copyEntriesFormer)
	//node.SetEntryCnt(int32(len(node.GetEntries(keyType))))
	node.SetEntries(node.GetEntries(keyType)[:idx+1])

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
}

func (node *SkipListBlockPage) GetSmallestKey(keyType types.TypeID) types.Value {
	return node.GetEntry(0, keyType).Key
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
}

func (node *SkipListBlockPage) GetEntryOffset(idx int) uint16 {
	offset := offsetEntryInfos + sizeEntryInfo*uint32(idx)

	return uint16(types.NewUInt16FromBytes(node.Data()[offset : offset+sizeEntryInfoOffset]))
}

func (node *SkipListBlockPage) SetEntryOffset(idx int, setOffset uint16) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, setOffset)
	setOffsetInBytes := buf.Bytes()
	offset := offsetEntryInfos + sizeEntryInfo*uint32(idx)
	copy(node.Data()[offset:], setOffsetInBytes)
}

func (node *SkipListBlockPage) GetEntrySize(idx int) uint16 {
	offset := offsetEntryInfos + sizeEntryInfo*uint32(idx) + sizeEntryInfoOffset

	return uint16(types.NewUInt16FromBytes(node.Data()[offset : offset+sizeEntryInfoSize]))
}

func (node *SkipListBlockPage) SetEntrySize(idx int, setSize uint16) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, setSize)
	setSizeInBytes := buf.Bytes()
	offset := offsetEntryInfos + sizeEntryInfo*uint32(idx) + sizeEntryInfoOffset
	copy(node.Data()[offset:], setSizeInBytes)
}

//func (node *SkipListBlockPage) GetEntrySize(idx int) uint16 {
//	offsetTgt := offsetEntryInfos + sizeEntryInfo*uint32(idx)
//	tgtOffset := uint16(types.NewUInt16FromBytes(node.Data()[offsetTgt : offsetTgt+sizeEntryInfoOffset]))
//
//	var prevOffset uint16
//	if idx == 0 {
//		// last entry
//		prevOffset = uint16(common.PageSize)
//	} else {
//		prevOffset = uint16(types.NewUInt16FromBytes(node.Data()[offsetTgt-sizeEntryInfoOffset : offsetTgt-sizeEntryInfoOffset+sizeEntryInfoOffset]))
//	}
//
//	return prevOffset - tgtOffset
//}

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
	node.SetEntryOffset(idx, uint16(newFSP))
	node.SetEntrySize(idx, uint16(entrySize))
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
		node.SetEntryOffset(ii, uint16(entryOffset))
		node.SetEntrySize(ii, uint16(entrySizes[ii]))
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
}

// since header space grow with insertion entry, memory size which is needed
// for insertion is not same with size of SkipListPair object
func (node *SkipListBlockPage) GetSpecifiedSLPNeedSpace(slp *SkipListPair) uint32 {
	return slp.GetDataSize() + sizeEntryInfo
}

// returns remaining bytes for additional entry
func (node *SkipListBlockPage) getFreeSpaceRemaining() uint32 {
	return (node.GetFreeSpacePointer() - 1) - (offsetEntryInfos + (sizeEntryInfo * uint32(node.GetEntryCnt())) - 1)
}

// TODO: (SDB) in concurrent impl, locking in this method is needed. and caller must do unlock (FectchAndCastToBlockPage)

// Attention:
//   caller must call UnpinPage with appropriate diaty page to the got page when page using ends
func FetchAndCastToBlockPage(bpm *buffer.BufferPoolManager, pageId types.PageID) *SkipListBlockPage {
	bPage := bpm.FetchPage(pageId)
	return (*SkipListBlockPage)(unsafe.Pointer(bPage))
}

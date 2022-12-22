package skip_list_page

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
//  ----------------------------------------------------------------------------------------------------------
//  | PageId (4)| LSN (4) | level (4)| entryCnt (4)| forward (4 * MAX_FOWARD_LIST_LEN) | FreeSpacePointer(4) |
//  ---------------------------------------------------------------------------------------------------------
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
//  for entry insertion cost is keeped lower
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
	sizeForward                         = uint32(4 * MAX_FOWARD_LIST_LEN)
	sizeForwardEntry                    = uint32(4) // types.PageID
	sizeFreeSpacePointer                = uint32(4)
	sizeEntryInfoOffset                 = uint32(2)
	sizeEntryInfoSize                   = uint32(2)
	sizeEntryInfo                       = sizeEntryInfoOffset + sizeEntryInfoSize
	sizeBlockPageHeaderExceptEntryInfos = sizePageId + sizeLevel + sizeEntryCnt + sizeForward + sizeFreeSpacePointer
	offsetPageId                        = int32(0)
	offsetLevel                         = sizePageId + types.SizeOfLSN
	offsetEntryCnt                      = offsetLevel + sizeLevel
	offsetForward                       = offsetEntryCnt + sizeEntryCnt
	offsetFreeSpacePointer              = offsetForward + sizeForward
	offsetEntryInfos                    = offsetFreeSpacePointer + sizeFreeSpacePointer
	sizeEntryValue                      = uint32(4)
)

type SkipListPair struct {
	Key   types.Value
	Value uint32
}

type SkipListCornerInfo struct {
	PageId        types.PageID
	UpdateCounter types.LSN
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
	ret.SetPageId(page_.GetPageId())
	ret.SetLSN(0)
	ret.SetEntryCnt(0)
	ret.SetLevel(level)
	ret.initForwardEntries()
	ret.SetFreeSpacePointer(common.PageSize)
	tmpSmallestListPair := smallestListPair
	// EntryCnt is incremented to 1
	ret.SetEntry(0, &tmpSmallestListPair)
	//ret.WLatch()
	//ret.AddWLatchRecord(int32(tmpSmallestListPair.Value))
	//bpm.FlushPage(ret.GetPageId())
	//bpm.UnpinPage(ret.GetPageId(), true)
	//ret.RemoveWLatchRecord(int32(tmpSmallestListPair.Value))
	//ret.WUnlatch()
	//// increment pin count because pin count is decremented on FlushPage
	//bpm.IncPinOfPage(ret)

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

// insert entry location info to page header next of idx index entry
// this method call slides memory area of header using memory copy
// idx==-1 -> data's index become 0 (insert to head of entries)
// idx==entryCnt -> data's index become entryCnt (insert next of last entry)
// ATTENTION:
//
//	caller should update entryCnt appropriatery after this method call
func (node *SkipListBlockPage) updateEntryInfosAtInsert(idx int, dataSize uint16) {
	// entrries data backward of entry which specifed with idx arg are not changed
	// because data of new entry is always placed tail of payload area

	// entrries info data backward of entry which specifed with idx arg are slided for
	// new entry insertion
	allEntryNum := uint32(node.GetEntryCnt())
	slideFromOffset := offsetEntryInfos + uint32(idx+1)*sizeEntryInfo
	slideToOffset := offsetEntryInfos + uint32(idx+2)*sizeEntryInfo
	slideAreaStartOffset := slideFromOffset
	slideAreaEndOffset := slideAreaStartOffset + (allEntryNum-uint32(idx+1))*sizeEntryInfo
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

func (node *SkipListBlockPage) getSplitIdxForNotFixed() (splitIdx_ int32, isNeedSplit bool) {
	halfOfmaxSize := int((common.PageSize - offsetEntryInfos) / 2)
	curSize := 0
	entryCnt := int(node.GetEntryCnt())
	splitIdx := -1
	for ii := 0; ii < entryCnt; ii++ {
		curSize = curSize + int(node.GetEntrySize(ii)) + int(sizeEntryInfo)
		if curSize >= halfOfmaxSize {
			splitIdx = ii
			break
		}
	}

	if splitIdx == entryCnt-1 {
		return int32(splitIdx), false
	} else {
		return int32(splitIdx), true
	}

}

// Attempts to insert a key and value into an index in the baccess
// return value is whether newNode is created or not
func (node *SkipListBlockPage) Insert(key *types.Value, value uint32, bpm *buffer.BufferPoolManager, corners []SkipListCornerInfo,
	level int32) (isNeedRetry_ bool) {
	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO, "Insert of SkipListBlockPage called! : key=%v\n", key.ToIFValue())
	}

	found, _, foundIdx := node.FindEntryByKey(key)
	isSuccess := false
	isSplited := false
	var lockedAndPinnedNodes []*SkipListBlockPage = nil
	var splitIdx int32 = -1
	if found {
		//fmt.Println("found at Insert")
		// over write exsiting entry
		if !node.GetEntry(int(foundIdx), key.ValueType()).Key.CompareEquals(*key) {
			panic("overwriting wrong entry!")
		}

		fmt.Println("Insert: key duplicatin occured")

		//if node.GetEntry(int(foundIdx), key.ValueType()).Key.CompareEquals(*key) {
		//	panic("key duplication is not supported yet!")
		//}
		
		node.SetEntry(int(foundIdx), &SkipListPair{*key, value})
		//fmt.Printf("end of Insert of SkipListBlockPage called! : key=%d page.entryCnt=%d len(page.entries)=%d\n", key.ToInteger(), node.entryCnt, len(node.entries))

		if node.GetEntry(int(foundIdx), key.ValueType()).Key.CompareEquals(*key) {
			node.SetEntryCnt(node.GetEntryCnt() - 1)
		}

		node.SetLSN(node.GetLSN() + 1)

		bpm.UnpinPage(node.GetPageId(), true)
		node.RemoveWLatchRecord(key.ToInteger())
		node.WUnlatch()
		if common.EnableDebug {
			common.ShPrintf(common.DEBUG_INFO, "SkipListBlockPage::Insert: finish (replace). key=%v\n", key.ToIFValue())
		}
		return false
	} else { // !found
		//fmt.Printf("not found at Insert of SkipListBlockPage. foundIdx=%d\n", foundIdx)
		if node.getFreeSpaceRemaining() < node.GetSpecifiedSLPNeedSpace(&SkipListPair{*key, value}) {
			// this node is full. so node split is needed

			// first, split this node at center of entry list

			common.ShPrintf(common.DEBUG_INFO, "SkipListBlockPage::Insert: node split occured!\n")

			isSplited = true
			isNeedSplitWithEntryMove := true

			if key.ValueType() == types.Varchar {
				// entries located post half data space are moved to new node
				splitIdx, isNeedSplitWithEntryMove = node.getSplitIdxForNotFixed()
			} else {
				// half of entries are moved to new node
				splitIdx = node.GetEntryCnt() / 2
			}

			if isNeedSplitWithEntryMove {
				// set this node as corner node of level-1
				corners[0] = SkipListCornerInfo{node.GetPageId(), node.GetLSN()}

				//bpm.UnpinPage(node.GetPageId(), false)
				node.RemoveWLatchRecord(key.ToInteger())
				node.WUnlatch()
				isSuccess, lockedAndPinnedNodes = validateNoChangeAndGetLock(bpm, corners[:level], nil)
				if !isSuccess {
					//bpm.UnpinPage(node.GetPageId(), false)
					//// already released lock of this node
					if common.EnableDebug {
						common.ShPrintf(common.DEBUG_INFO, "SkipListBlockPage::Insert: finish (validation NG). key=%v\n", key.ToIFValue())
					}
					return true
				}
				//*node = *lockedAndPinnedNodes[0]
				//} else {
				//node.DecPinCount()
				bpm.DecPinOfPage(node)
				//}

				newNode := node.SplitNode(splitIdx, bpm, corners, level, key.ValueType(), lockedAndPinnedNodes)
				// keep having Wlatch and pin of newNode and this node only here

				if foundIdx > splitIdx {
					// insert to new node
					newSmallerIdx := foundIdx - splitIdx - 1
					//newNodePageId := node.GetForwardEntry(0)
					//newNode := FetchAndCastToBlockPage(bpm, newNodePageId)

					insEntry := &SkipListPair{*key, value}
					if key.ValueType() == types.Varchar && (newNode.GetSpecifiedSLPNeedSpace(insEntry) > newNode.getFreeSpaceRemaining()) {
						panic("not enough space for insert (after node split)")
					}
					newNode.InsertInner(int(newSmallerIdx), insEntry)
					bpm.UnpinPage(newNode.GetPageId(), true)
					newNode.RemoveWLatchRecord(-200000)
					newNode.WUnlatch()
					//fmt.Printf("end of Insert of SkipListBlockPage called! : key=%d page.entryCnt=%d len(page.entries)=%d\n", key.ToInteger(), node.entryCnt, len(node.entries))

					//node.SetLSN(node.GetLSN() + 1)
					//unlockAndUnpinNodes(bpm, lockedAndPinnedNodes, true)

					bpm.UnpinPage(node.GetPageId(), true)
					node.RemoveWLatchRecord(key.ToInteger())
					node.WUnlatch()
					if common.EnableDebug {
						common.ShPrintf(common.DEBUG_INFO, "SkipListBlockPage::Insert: finish (split & insert to new node). key=%v\n", key.ToIFValue())
					}
					return false
				} else {
					// insert to this node
					// foundIdx is index of nearlest smaller key entry
					// new entry is inserted next of the entry

					insEntry := &SkipListPair{*key, value}
					if key.ValueType() == types.Varchar && (node.GetSpecifiedSLPNeedSpace(insEntry) > node.getFreeSpaceRemaining()) {
						if isSplited {
							panic("not enough space for insert (after node split)")
						} else {
							panic("not enough space for insert.")
						}
					}
					node.InsertInner(int(foundIdx), insEntry)

					//unlockAndUnpinNodes(bpm, lockedAndPinnedNodes, true)

					bpm.UnpinPage(newNode.GetPageId(), true)
					newNode.RemoveWLatchRecord(-200000)
					newNode.WUnlatch()
					bpm.UnpinPage(node.GetPageId(), true)
					node.RemoveWLatchRecord(key.ToInteger())
					node.WUnlatch()

					if common.EnableDebug {
						common.ShPrintf(common.DEBUG_INFO, "SkipListBlockPage::Insert: finish (new ently only split). key=%v\n", key.ToIFValue())
					}

					return false
				}
			} else {
				// new entry only inserted new node

				// set this node as corner node of level-1
				corners[0] = SkipListCornerInfo{node.GetPageId(), node.GetLSN()}

				//bpm.UnpinPage(node.GetPageId(), false)
				node.RemoveWLatchRecord(key.ToInteger())
				node.WUnlatch()
				isSuccess, lockedAndPinnedNodes = validateNoChangeAndGetLock(bpm, corners[:level], nil)
				//bpm.UnpinPage(node.GetPageId(), false)
				//node.DecPinCount()
				//bpm.DecPinOfPage(node)
				if !isSuccess {
					// already released lock of this node
					if common.EnableDebug {
						common.ShPrintf(common.DEBUG_INFO, "SkipListBlockPage::Insert: finish (validation NG). key=%v\n", key.ToIFValue())
					}
					return true
				}
				bpm.DecPinOfPage(node)
				//*node = *lockedAndPinnedNodes[0]

				//corners[0] = SkipListCornerInfo{node.GetPageId(), node.GetLSN()}
				//node.WUnlatch()
				insEntry := &SkipListPair{*key, value}
				newNode := node.splitWithoutEntryMove(bpm, corners, level, key.ValueType(), lockedAndPinnedNodes, insEntry)
				// keep having Wlatch and pin of newNode and this node only here

				bpm.UnpinPage(newNode.GetPageId(), true)
				newNode.RemoveWLatchRecord(-200000)
				newNode.WUnlatch()

				//unlockAndUnpinNodes(bpm, lockedAndPinnedNodes, true)
				bpm.UnpinPage(node.GetPageId(), true)
				node.RemoveWLatchRecord(key.ToInteger())
				node.WUnlatch()

				if common.EnableDebug {
					common.ShPrintf(common.DEBUG_INFO, "SkipListBlockPage::Insert: finish (new node without split). key=%v\n", key.ToIFValue())
				}
				return false
			}
		} else {
			// no split
			if common.EnableDebug && common.ActiveLogKindSetting&common.DEBUGGING > 0 {
				bpm.PrintReplacerInternalState()
			}

			//fmt.Printf("end of Insert of SkipListBlockPage called! : key=%d page.entryCnt=%d len(page.entries)=%d\n", key.ToInteger(), node.entryCnt, len(node.entries))
			node.SetLSN(node.GetLSN() + 1)

			insEntry := &SkipListPair{*key, value}
			node.InsertInner(int(foundIdx), insEntry)

			bpm.UnpinPage(node.GetPageId(), true)
			node.RemoveWLatchRecord(key.ToInteger())
			node.WUnlatch()
			if common.EnableDebug {
				common.ShPrintf(common.DEBUG_INFO, "SkipListBlockPage::Insert: finish (no split). key=%v\n", key.ToIFValue())
			}
			return false
		}
	}
}

// remove entry info specified with idx arg from header of page
// this method slides memory area of header using memory copy
// ATTENTION:
//
//	caller should update entryCnt appropriatery after this method call
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

// ATTENTION:
// nodes corresponding to checkNodes and additionalCheckNode must not be locked
//
// Note:
// additionalCheckNode arg is used only at node remove operation
//
// return value:
// isSuscess == true
// => nodes on lockedAndPinnedNodes (= checkNodes) are locked and pinned
// isSuccess == false
// => lockedAndPinnedNodes is nil and nodes on lockedAndPinnedNodes (= checkNodes) are unlocked and unpinned
func validateNoChangeAndGetLock(bpm *buffer.BufferPoolManager, checkNodes []SkipListCornerInfo, additonalCheckNode *SkipListCornerInfo) (isSuccess bool, lockedAndPinnedNodes []*SkipListBlockPage) {
	common.ShPrintf(common.DEBUG_INFO, "validateNoChangeAndGetLock: start. len(checkNodes)=%d\n", len(checkNodes))
	checkLen := len(checkNodes)
	validatedNodes := make([]*SkipListBlockPage, 0)
	prevPageId := types.InvalidPageID
	for ii := checkLen - 1; ii >= 0; ii-- {
		var node *SkipListBlockPage
		if checkNodes[ii].PageId == prevPageId {
			node = validatedNodes[0]
		} else {
			node = FetchAndCastToBlockPage(bpm, checkNodes[ii].PageId)
		}

		isPassed := false
		if node == nil {
			common.ShPrintf(common.DEBUG_INFO, "validateNoChangeAndGetLock: validation failed. go retry.\n")
			unlockAndUnpinNodes(bpm, validatedNodes, false)

			if additonalCheckNode != nil {
				bpm.UnpinPage(additonalCheckNode.PageId, false)
			} else {
				bpm.UnpinPage(checkNodes[0].PageId, false)
			}
			return false, nil
		}

		//if node.GetPageId() == prevPageId {
		//	////unpin because fetched same page
		//	//bpm.UnpinPage(node.GetPageId(), true)
		//	//node.DecPinCount()
		//	bpm.DecPinOfPage(node)
		//	isPassed = true
		//	// locking is not needed because it has already got
		//} else {
		//	node.WLatch()
		//}
		if node.GetPageId() == prevPageId {
			isPassed = true
		} else {
			node.WLatch()
			node.AddWLatchRecord(-10)
		}

		if !isPassed {
			tmpNodes := make([]*SkipListBlockPage, 0)
			tmpNodes = append(tmpNodes, node)
			validatedNodes = append(tmpNodes, validatedNodes...)
		}

		// LSN is used for update counter
		if node.GetLSN() != checkNodes[ii].UpdateCounter {
			common.ShPrintf(common.DEBUG_INFO, "validateNoChangeAndGetLock: validation is NG: go retry. len(validatedNodes)=%d\n", len(validatedNodes))
			unlockAndUnpinNodes(bpm, validatedNodes, false)
			if additonalCheckNode != nil {
				bpm.UnpinPage(additonalCheckNode.PageId, false)
			} else {
				bpm.UnpinPage(checkNodes[0].PageId, false)
			}
			return false, nil
		}

		prevPageId = checkNodes[ii].PageId
	}

	// additionalCheckNode is remove node at Remove method currently
	if additonalCheckNode != nil {
		node := FetchAndCastToBlockPage(bpm, additonalCheckNode.PageId)
		if node == nil {
			common.ShPrintf(common.DEBUG_INFO, "validateNoChangeAndGetLock: additionalCheckNode validation failed. go retry.\n")
			unlockAndUnpinNodes(bpm, validatedNodes, false)
			return false, nil
		}
		node.WLatch()
		node.AddWLatchRecord(-10)
		if node.GetLSN() != additonalCheckNode.UpdateCounter {
			common.ShPrintf(common.DEBUG_INFO, "validateNoChangeAndGetLock: validation of additionalCheckNode is NG: go retry. len(validatedNodes)=%d\n", len(validatedNodes))
			//bpm.UnpinPage(node.GetPageId(), true)
			//node.DecPinCount()
			bpm.DecPinOfPage(node)
			bpm.UnpinPage(node.GetPageId(), true)
			node.RemoveWLatchRecord(-10)
			node.WUnlatch()
			unlockAndUnpinNodes(bpm, validatedNodes, false)
			return false, nil
		}
		//bpm.UnpinPage(node.GetPageId(), true)
		validatedNodes = append(validatedNodes, node)
	}

	common.ShPrintf(common.DEBUG_INFO, "validateNoChangeAndGetLock: finish. len(validatedNodes)=%d\n", len(validatedNodes))
	// validation is passed
	return true, validatedNodes
}

func unlockAndUnpinNodes(bpm *buffer.BufferPoolManager, checkedNodes []*SkipListBlockPage, isDirty bool) {
	common.ShPrintf(common.DEBUG_INFO, "unlockAndUnpinNodes: start. len(checkNodes)=%d\n", len(checkedNodes))
	for _, curNode := range checkedNodes {
		curPageId := curNode.GetPageId()
		bpm.UnpinPage(curPageId, isDirty)
		curNode.RemoveWLatchRecord(-10)
		curNode.WUnlatch()
	}
	common.ShPrintf(common.DEBUG_INFO, "unlockAndUnpinNodes: finished. len(checkNodes)=%d\n", len(checkedNodes))
}

func (node *SkipListBlockPage) Remove(bpm *buffer.BufferPoolManager, key *types.Value, predOfCorners []SkipListCornerInfo, corners []SkipListCornerInfo) (isNodeShouldBeDeleted bool, isDeleted bool, isNeedRetry bool) {
	if common.EnableDebug && common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
		fmt.Printf("SkipListBlockPage::Remove: start. key=%v\n", key.ToIFValue())
	}
	found, _, foundIdx := node.FindEntryByKey(key)
	if found && (node.GetEntryCnt() == 1) {
		if !node.GetEntry(0, key.ValueType()).Key.CompareEquals(*key) {
			panic("removing wrong entry!")
		}

		if common.EnableDebug && common.ActiveLogKindSetting&common.DEBUG_INFO > 0 {
			fmt.Printf("SkipListBlockPage::Remove: node remove occured!\n")
		}

		updateLen := int(node.GetLevel())

		checkNodes := make([]SkipListCornerInfo, 0)
		checkNodes = append(checkNodes, predOfCorners[0])
		checkNodes = append(checkNodes, corners[1:updateLen]...)
		// check of thid node is also needed
		additionalCheckNode := &SkipListCornerInfo{node.GetPageId(), node.GetLSN()}
		//bpm.UnpinPage(node.GetPageId(), false)
		node.RemoveWLatchRecord(key.ToInteger())
		node.WUnlatch()
		isSuccess, lockedAndPinnedNodes := validateNoChangeAndGetLock(bpm, checkNodes, additionalCheckNode)
		//bpm.UnpinPage(node.GetPageId(), true)
		//node.DecPinCount()
		if !isSuccess {
			// already released all lock and pin which includes this node

			//// because WUnlatch is already called once before validateNoChangeAndGetLock func call, but  pin is not released
			//bpm.UnpinPage(node.GetPageId(), true)

			return false, false, true
		}
		bpm.DecPinOfPage(node)
		//*node = *lockedAndPinnedNodes[len(lockedAndPinnedNodes)-1]

		// removing this node from all level of chain
		for ii := 1; ii < updateLen; ii++ {
			//corner := FetchAndCastToBlockPage(bpm, corners[ii].PageId)
			corner := FindSLBPFromList(lockedAndPinnedNodes, corners[ii].PageId)
			corner.SetForwardEntry(ii, node.GetForwardEntry(ii))
			corner.SetLSN(corner.GetLSN() + 1)
			//bpm.UnpinPage(corners[ii].PageId, true)
			//corner.DecPinCount()
			//bpm.DecPinOfPage(corner)
		}
		// level-1's pred is stored predOfCorners
		//pred := FetchAndCastToBlockPage(bpm, predOfCorners[0].PageId)
		pred := FindSLBPFromList(lockedAndPinnedNodes, predOfCorners[0].PageId)
		pred.SetForwardEntry(0, node.GetForwardEntry(0))
		pred.SetLSN(pred.GetLSN() + 1)
		////bpm.UnpinPage(predOfCorners[0].PageId, true)
		////pred.DecPinCount()
		//bpm.DecPinOfPage(pred)
		node.SetLSN(node.GetLSN() + 1)

		unlockAndUnpinNodes(bpm, lockedAndPinnedNodes, true)
		//node.WUnlatch()
		//bpm.UnpinPage(node.GetPageId(), true)

		if common.EnableDebug {
			common.ShPrintf(common.DEBUG_INFO, "SkipListBlockPage::Remove: finished (node remove). key=%v\n", key.ToIFValue())
		}
		//// because WUnlatch is already called once before validateNoChangeAndGetLock func call, but  pin is not released ???
		//bpm.UnpinPage(node.GetPageId(), true)

		return true, true, false
	} else if found {
		if !node.GetEntry(int(foundIdx), key.ValueType()).Key.CompareEquals(*key) {
			panic("removing wrong entry!")
		}

		node.RemoveInner(int(foundIdx))

		node.SetLSN(node.GetLSN() + 1)
		bpm.UnpinPage(node.GetPageId(), true)
		node.RemoveWLatchRecord(key.ToInteger())
		node.WUnlatch()
		if common.EnableDebug {
			common.ShPrintf(common.DEBUG_INFO, "SkipListBlockPage::Remove: finished (found). key=%v\n", key.ToIFValue())
		}
		return false, true, false
	} else { // found == false
		bpm.UnpinPage(node.GetPageId(), true)
		node.RemoveWLatchRecord(key.ToInteger())
		node.WUnlatch()
		// do nothing
		if common.EnableDebug {
			common.ShPrintf(common.DEBUG_INFO, "SkipListBlockPage::Remove: finished (not found). key=%v\n", key.ToIFValue())
		}
		return false, false, false
	}
}

// create new node and update chain
// ATTENTION:
// after this method call current thread hold wlatch of "node" and newNode only
// and these are pinned
func (node *SkipListBlockPage) splitWithoutEntryMove(bpm *buffer.BufferPoolManager, corners []SkipListCornerInfo,
	level int32, keyType types.TypeID, lockedAndPinnedNodes []*SkipListBlockPage, insertEntry *SkipListPair) (newNode_ *SkipListBlockPage) {
	//fmt.Println("<<<<<<<<<<<<<<<<<<<<<<<< SplitNode called! >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

	newNode := node.newNodeAndUpdateChain(-1, bpm, corners, level, keyType, lockedAndPinnedNodes, insertEntry)
	// having lock and pin of "node" and newNode here

	newNode.SetLevel(level)

	return newNode
}

// split entries of node at entry specified with idx arg
// new node contains entries node.entries[idx+1:]
// (new node does not include entry node.entries[idx])
// ATTENTION:
// after this method call current thread hold wlatch of "node" and newNode only
// and these are pinned
func (node *SkipListBlockPage) SplitNode(idx int32, bpm *buffer.BufferPoolManager, corners []SkipListCornerInfo,
	level int32, keyType types.TypeID, lockedAndPinnedNodes []*SkipListBlockPage) (newNode_ *SkipListBlockPage) {
	//fmt.Println("<<<<<<<<<<<<<<<<<<<<<<<< SplitNode called! >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

	newNode := node.newNodeAndUpdateChain(idx, bpm, corners, level, keyType, lockedAndPinnedNodes, nil)
	// having lock and pin of newNode here

	newNode.SetEntries(node.GetEntries(keyType)[idx+1:])
	//newNode.SetLevel(level)
	node.SetEntries(node.GetEntries(keyType)[:idx+1])

	return newNode
}

func FindSLBPFromList(list []*SkipListBlockPage, pageID types.PageID) *SkipListBlockPage {
	for _, t := range list {
		if t.GetPageId() == pageID {
			return t
		}
	}
	return nil
}

func (node *SkipListBlockPage) newNodeAndUpdateChain(idx int32, bpm *buffer.BufferPoolManager, corners []SkipListCornerInfo, level int32, keyType types.TypeID, lockedAndPinnedNodes []*SkipListBlockPage, insertEntry *SkipListPair) *SkipListBlockPage {
	var newNode *SkipListBlockPage
	if insertEntry != nil {
		newNode = NewSkipListBlockPage(bpm, level, *insertEntry)
	} else {
		newNode = NewSkipListBlockPage(bpm, level, *node.GetEntry(int(idx+1), keyType))
	}

	newNode.WLatch()
	newNode.AddWLatchRecord(-200000)

	for ii := 0; ii < int(level); ii++ {
		// modify forward link
		var tmpNode *SkipListBlockPage
		if corners[ii].PageId == node.GetPageId() {
			tmpNode = node
		} else {
			fetchedPage := FindSLBPFromList(lockedAndPinnedNodes, corners[ii].PageId)
			if fetchedPage != nil {
				tmpNode = fetchedPage
			} else {
				panic("update check functon musy have bug!")
			}
		}
		newNode.SetForwardEntry(ii, tmpNode.GetForwardEntry(ii))
		tmpNode.SetForwardEntry(ii, newNode.GetPageId())
		tmpNode.SetLSN(tmpNode.GetLSN() + 1)
		//bpm.UnpinPage(tmpNode.GetPageId(), true)
		//tmpNode.DecPinCount()
		//if corners[ii].PageId != node.GetPageId() {
		//	bpm.DecPinOfPage(tmpNode)
		//}
	}

	// release latches and pins except current updating node ("node" receiver object)
	modNodeId := node.GetPageId()
	for ii := len(lockedAndPinnedNodes) - 1; ii > 0; ii-- {
		curPageId := lockedAndPinnedNodes[ii].GetPageId()
		if curPageId != modNodeId {
			bpm.UnpinPage(lockedAndPinnedNodes[ii].GetPageId(), true)
			lockedAndPinnedNodes[ii].RemoveWLatchRecord(-10)
			lockedAndPinnedNodes[ii].WUnlatch()
		}
	}
	return newNode
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

func (node *SkipListBlockPage) GetForwardEntry(idx int) types.PageID {
	return types.NewPageIDFromBytes(node.Data()[offsetForward+uint32(idx)*sizeForwardEntry:])
}

func (node *SkipListBlockPage) SetForwardEntry(idx int, fwdNodeId types.PageID) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, fwdNodeId)
	fwdNodeIdInBytes := buf.Bytes()
	copy(node.Data()[offsetForward+uint32(idx)*sizeForwardEntry:], fwdNodeIdInBytes)
}

func (node *SkipListBlockPage) GetEntryCnt() int32 {
	return int32(types.NewInt32FromBytes(node.Data()[offsetEntryCnt:]))
}

func (node *SkipListBlockPage) SetEntryCnt(cnt int32) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, cnt)
	cntInBytes := buf.Bytes()
	copy(node.Data()[offsetEntryCnt:], cntInBytes)
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

// note: freeSpacePointer value is index of buffer which points already data placed
//
//	so, you can use memory Data()[someOffset:freeSpacePointer] in golang description
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
//   - it is guranteed that new entry insert doesn't cause overflow of node space capacity
//   - key of target entry doesn't exist in this node
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

// since header space grow with insertion entry, memory size which is needed
// for insertion is not same with size of SkipListPair object
func (node *SkipListBlockPage) GetSpecifiedSLPNeedSpace(slp *SkipListPair) uint32 {
	return slp.GetDataSize() + sizeEntryInfo
}

// returns remaining bytes for additional entry
func (node *SkipListBlockPage) getFreeSpaceRemaining() uint32 {
	return (node.GetFreeSpacePointer() - 1) - (offsetEntryInfos + (sizeEntryInfo * uint32(node.GetEntryCnt())) - 1)
}

// Attention:
//
//	caller must call UnpinPage with appropriate diaty page to the got page when page using ends
func FetchAndCastToBlockPage(bpm *buffer.BufferPoolManager, pageId types.PageID) *SkipListBlockPage {
	bPage := bpm.FetchPage(pageId)
	if bPage == nil {
		// target page is physically removed (deallocated)
		return nil
	}
	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO, "FetchAndCastToBlockPage: PageId=%d PinCount=%d\n", bPage.GetPageId(), bPage.PinCount())
	}
	return (*SkipListBlockPage)(unsafe.Pointer(bPage))
}

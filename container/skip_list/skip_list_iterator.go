package skip_list

import (
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/page/skip_list_page"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
)

type SkipListIterator struct {
	sl       *SkipList
	bpm      *buffer.BufferPoolManager
	curNode  *skip_list_page.SkipListBlockPage
	curEntry *skip_list_page.SkipListPair
	// TODO: (SDB) curPageSlotIdx should be change to method local variable
	curPageSlotIdx int32
	rangeStartKey  *types.Value
	rangeEndKey    *types.Value
	keyType        types.TypeID
	ridList        []page.RID
	curRIDIdx      int32
}

// TODO: (SDB) cuncurrent iterator need RID list when iterator is created

func NewSkipListIterator(sl *SkipList, rangeStartKey *types.Value, rangeEndKey *types.Value) *SkipListIterator {
	ret := new(SkipListIterator)

	//headerPage := skip_list_page.FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)
	headerPage := sl.getHeaderPage()

	ret.sl = sl
	ret.bpm = sl.bpm
	//ret.curNode = skip_list_page.FetchAndCastToBlockPage(sl.bpm, headerPage.GetListStartPageId())

	ret.curPageSlotIdx = 0
	ret.rangeStartKey = rangeStartKey
	ret.rangeEndKey = rangeEndKey
	ret.keyType = headerPage.GetKeyType()
	ret.ridList = make([]page.RID, 0)

	//if rangeStartKey != nil {
	//	//sl.bpm.UnpinPage(headerPage.GetListStartPageId(), false)
	//	ret.curNode = nil
	//}

	//sl.bpm.UnpinPage(sl.headerPageID, false)
	ret.initRIDList(sl)

	return ret
}

func (itr *SkipListIterator) initRIDList(sl *SkipList) {
	// TODO: (SDB) need modify latching granularity

	// set appropriate start position
	if itr.rangeStartKey != nil {
		var corners []skip_list_page.SkipListCornerInfo
		_, itr.curPageSlotIdx, _, corners = itr.sl.FindNodeWithEntryIdxForItr(itr.rangeStartKey)
		// locking is not needed because already have lock with FindNodeWithEntryIdxForItr method call
		itr.curNode = skip_list_page.FetchAndCastToBlockPage(itr.bpm, corners[0].PageId)

		// this Unpin is needed due to already having one pin with FindNodeWithEntryIdxForItr method call
		itr.bpm.UnpinPage(corners[0].PageId, false)
		// release lock which is got on FindNodeWithEntryIdxForItr method
		itr.curNode.RUnlatch()
	} else {
		itr.curNode = sl.getStartNode()
		// for keepping pin count is one after iterator finishd using startNode
		sl.bpm.IncPinOfPage(itr.curNode)
	}

	for {
		itr.curNode.RLatch()
		if itr.curPageSlotIdx+1 >= itr.curNode.GetEntryCnt() {
			prevNodeId := itr.curNode.GetPageId()
			nextNodeId := itr.curNode.GetForwardEntry(0)
			if prevNodeId != itr.sl.getStartNode().GetPageId() {
				itr.bpm.UnpinPage(prevNodeId, false)
			}
			itr.curNode.RUnlatch()
			itr.curNode = skip_list_page.FetchAndCastToBlockPage(itr.bpm, nextNodeId)
			itr.curPageSlotIdx = -1
			itr.curNode.RLatch()
			if itr.curNode.GetSmallestKey(itr.keyType).IsInfMax() {
				// reached tail node
				if itr.curNode.GetPageId() != itr.sl.getStartNode().GetPageId() {
					itr.bpm.UnpinPage(itr.curNode.GetPageId(), false)
				}
				itr.curNode.RUnlatch()
				//return true, nil, nil, math.MaxUint32
				break
			}
		}

		// always having RLatch of itr.curNode

		itr.curPageSlotIdx++

		if itr.rangeEndKey != nil && itr.curNode.GetEntry(int(itr.curPageSlotIdx), itr.keyType).Key.CompareGreaterThan(*itr.rangeEndKey) {
			itr.bpm.UnpinPage(itr.curNode.GetPageId(), false)
			itr.curNode.RUnlatch()
			//return true, nil, nil, math.MaxUint32
			break
		}

		tmpVal := itr.curNode.GetEntry(int(itr.curPageSlotIdx), itr.keyType).Value
		itr.curNode.RUnlatch()
		itr.ridList = append(itr.ridList, samehada_util.UnpackUint32toRID(tmpVal))
		//return false, nil, &tmpKey, itr.curNode.GetEntry(int(itr.curPageSlotIdx), itr.keyType).Value
	}
}

//// ATTENTION:
//// caller must call this until getting "done" is true
//func (itr *SkipListIterator) Next() (done bool, err error, key *types.Value, val uint32) {
//
//	if itr.rangeStartKey != nil && itr.curNode == nil {
//		var corners []skip_list_page.SkipListCornerInfo
//		_, itr.curPageSlotIdx, _, corners = itr.sl.FindNodeWithEntryIdxForItr(itr.rangeStartKey)
//		// locking is not needed because already have lock with FindNodeWithEntryIdxForItr method call
//		itr.curNode = skip_list_page.FetchAndCastToBlockPage(itr.bpm, corners[0].PageId)
//		// this Unpin is needed due to already having one pin with FindNodeWithEntryIdxForItr method call
//		itr.bpm.UnpinPage(corners[0].PageId, false)
//
//		// release lock which is got on FindNodeWithEntryIdxForItr method
//		itr.curNode.RUnlatch()
//	}
//
//	itr.curNode.RLatch()
//	if itr.curPageSlotIdx+1 >= itr.curNode.GetEntryCnt() {
//		prevNodeId := itr.curNode.GetPageId()
//		nextNodeId := itr.curNode.GetForwardEntry(0)
//		if prevNodeId != itr.sl.getStartNode().GetPageId() {
//			itr.bpm.UnpinPage(prevNodeId, false)
//		}
//		itr.curNode.RUnlatch()
//		itr.curNode = skip_list_page.FetchAndCastToBlockPage(itr.bpm, nextNodeId)
//		itr.curPageSlotIdx = -1
//		itr.curNode.RLatch()
//		if itr.curNode.GetSmallestKey(itr.keyType).IsInfMax() {
//			// reached tail node
//			if itr.curNode.GetPageId() != itr.sl.getStartNode().GetPageId() {
//				itr.bpm.UnpinPage(itr.curNode.GetPageId(), false)
//			}
//			itr.curNode.RUnlatch()
//			return true, nil, nil, math.MaxUint32
//		}
//	}
//
//	// always having RLatch of itr.curNode
//
//	itr.curPageSlotIdx++
//
//	if itr.rangeEndKey != nil && itr.curNode.GetEntry(int(itr.curPageSlotIdx), itr.keyType).Key.CompareGreaterThan(*itr.rangeEndKey) {
//		itr.bpm.UnpinPage(itr.curNode.GetPageId(), false)
//		itr.curNode.RUnlatch()
//		return true, nil, nil, math.MaxUint32
//	}
//
//	tmpKey := itr.curNode.GetEntry(int(itr.curPageSlotIdx), itr.keyType).Key
//	itr.curNode.RUnlatch()
//	return false, nil, &tmpKey, itr.curNode.GetEntry(int(itr.curPageSlotIdx), itr.keyType).Value
//}

func (itr *SkipListIterator) Next() (done bool, err error, key *types.Value, val uint32) {
	// TODO: (SDB) not implemented yet (SkipListIterator::Next)
	return true, nil, nil, math.MaxUint32
}

package skip_list

import (
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/page/skip_list_page"
	"github.com/ryogrid/SamehadaDB/types"
)

type SkipListIterator struct {
	sl            *SkipList
	bpm           *buffer.BufferPoolManager
	curNode       *skip_list_page.SkipListBlockPage
	curEntry      *skip_list_page.SkipListPair
	rangeStartKey *types.Value
	rangeEndKey   *types.Value
	keyType       types.TypeID
	entryList     []*skip_list_page.SkipListPair
	curEntryIdx   int32
}

func NewSkipListIterator(sl *SkipList, rangeStartKey *types.Value, rangeEndKey *types.Value) *SkipListIterator {
	ret := new(SkipListIterator)

	headerPage := sl.getHeaderPage()

	ret.sl = sl
	ret.bpm = sl.bpm

	ret.rangeStartKey = rangeStartKey
	ret.rangeEndKey = rangeEndKey
	ret.keyType = headerPage.GetKeyType()
	ret.entryList = make([]*skip_list_page.SkipListPair, 0)

	ret.initRIDList(sl)

	return ret
}

func (itr *SkipListIterator) initRIDList(sl *SkipList) {
	curPageSlotIdx := int32(0)
	// set appropriate start position
	if itr.rangeStartKey != nil {
		found, node, slotIdx := itr.sl.FindNodeWithEntryIdxForItr(itr.rangeStartKey)
		// locking is not needed because already have lock with FindNodeWithEntryIdxForItr method call
		if found {
			// considering increment of curPageSlotIdx after here
			// because slotIdx is match indx of rangeStartKey
			curPageSlotIdx = slotIdx - 1
		} else {
			// considering increment of curPageSlotIdx after here
			// because slotIdx is nearest smaller key of rangeStartKey
			curPageSlotIdx = slotIdx
		}
		itr.curNode = node
	} else {
		itr.curNode = sl.getStartNode()
		itr.curNode.RLatch()
		itr.curNode.AddRLatchRecord(-10000)
		// for keepping pin count is one after iterator finishd using startNode
		sl.bpm.IncPinOfPage(itr.curNode)
	}

	for {
		if curPageSlotIdx+1 >= itr.curNode.GetEntryCnt() {
			prevNodeId := itr.curNode.GetPageId()
			nextNodeId := itr.curNode.GetForwardEntry(0)
			prevNode := itr.curNode
			itr.curNode = skip_list_page.FetchAndCastToBlockPage(itr.bpm, nextNodeId)
			itr.curNode.RLatch()
			itr.curNode.AddRLatchRecord(-10000)
			itr.bpm.UnpinPage(prevNodeId, false)
			prevNode.RemoveRLatchRecord(-10000)
			prevNode.RUnlatch()
			curPageSlotIdx = -1
			if itr.curNode.GetSmallestKey(itr.keyType).IsInfMax() {
				// reached tail node
				itr.bpm.UnpinPage(itr.curNode.GetPageId(), false)
				itr.curNode.RemoveRLatchRecord(-10000)
				itr.curNode.RUnlatch()
				break
			}
		}

		// always having RLatch of itr.curNode

		curPageSlotIdx++

		if itr.rangeEndKey != nil && itr.curNode.GetEntry(int(curPageSlotIdx), itr.keyType).Key.CompareGreaterThan(*itr.rangeEndKey) {
			itr.bpm.UnpinPage(itr.curNode.GetPageId(), false)
			itr.curNode.RemoveRLatchRecord(-10000)
			itr.curNode.RUnlatch()
			break
		}

		itr.entryList = append(itr.entryList, itr.curNode.GetEntry(int(curPageSlotIdx), itr.keyType))
	}
}

func (itr *SkipListIterator) Next() (done bool, err error, key *types.Value, rid *page.RID) {
	if itr.curEntryIdx < int32(len(itr.entryList)) {
		ret := itr.entryList[itr.curEntryIdx]
		itr.curEntryIdx++
		tmpRID := samehada_util.UnpackUint32toRID(ret.Value)
		return false, nil, samehada_util.GetPonterOfValue(ret.Key), &tmpRID
	} else {
		return true, nil, nil, nil
	}
}

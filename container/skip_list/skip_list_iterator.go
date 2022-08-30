package skip_list

import (
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page/skip_list_page"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
)

type SkipListIterator struct {
	sl            *SkipList
	bpm           *buffer.BufferPoolManager
	curNode       *skip_list_page.SkipListBlockPage
	curEntry      *skip_list_page.SkipListPair
	curIdx        int32
	rangeStartKey *types.Value
	rangeEndKey   *types.Value
	keyType       types.TypeID
}

// TODO: (SDB) cuncurrent iterator need RID list when iterator is created

// ATTENTION:
// caller must call this until getting "done" is true
func (itr *SkipListIterator) Next() (done bool, err error, key *types.Value, val uint32) {
	if itr.rangeStartKey != nil && itr.curNode == nil {
		var corners []skip_list_page.SkipListCornerInfo
		itr.curIdx, _, corners = itr.sl.FindNodeWithEntryIdxForItr(itr.rangeStartKey)
		itr.curNode = skip_list_page.FetchAndCastToBlockPage(itr.bpm, corners[0].PageId)
		// this Unpin is needed due to already having one pin with FindNodeWithEntryIdxForItr method call
		itr.bpm.UnpinPage(corners[0].PageId, false)
		// locking is not needed because already have lock with FindNodeWithEntryIdxForItr method call
	}

	if itr.curIdx+1 >= itr.curNode.GetEntryCnt() {
		prevNodeId := itr.curNode.GetPageId()
		itr.curNode.RUnlock()
		itr.curNode = skip_list_page.FetchAndCastToBlockPage(itr.bpm, itr.curNode.GetForwardEntry(0))
		itr.bpm.UnpinPage(prevNodeId, false)
		itr.curIdx = -1
		itr.curNode.RLock()
		if itr.curNode.GetSmallestKey(itr.keyType).IsInfMax() {
			// reached tail node
			itr.curNode.RUnlock()
			itr.bpm.UnpinPage(itr.curNode.GetPageId(), false)
			return true, nil, nil, math.MaxUint32
		}
		itr.curNode.RUnlock()
	}

	itr.curIdx++
	itr.curNode.RLock()

	if itr.rangeEndKey != nil && itr.curNode.GetEntry(int(itr.curIdx), itr.keyType).Key.CompareGreaterThan(*itr.rangeEndKey) {
		itr.curNode.RUnlock()
		itr.bpm.UnpinPage(itr.curNode.GetPageId(), false)
		return true, nil, nil, math.MaxUint32
	}

	tmpKey := itr.curNode.GetEntry(int(itr.curIdx), itr.keyType).Key
	return false, nil, &tmpKey, itr.curNode.GetEntry(int(itr.curIdx), itr.keyType).Value
}

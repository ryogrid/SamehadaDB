package skip_list

import (
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page/skip_list_page"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
)

type SkipListIterator struct {
	bpm           *buffer.BufferPoolManager
	curNode       *skip_list_page.SkipListBlockPage
	curEntry      *skip_list_page.SkipListPair
	curIdx        int32
	rangeStartKey *types.Value
	rangeEndKey   *types.Value
	keyType       types.TypeID
}

func (itr *SkipListIterator) Next() (done bool, err error, key *types.Value, val uint32) {
	if itr.curIdx+1 >= itr.curNode.GetEntryCnt() {
		prevNodeId := itr.curNode.GetPageId()
		itr.curNode = skip_list_page.FetchAndCastToBlockPage(itr.bpm, itr.curNode.GetForwardEntry(0))
		itr.bpm.UnpinPage(prevNodeId, false)
		for itr.curNode.GetIsNeedDeleted() && !itr.curNode.GetBiggestKey(itr.keyType).IsInfMax() {
			// skip isNeedDeleted marked node
			prevNodeId = itr.curNode.GetPageId()
			itr.curNode = skip_list_page.FetchAndCastToBlockPage(itr.bpm, itr.curNode.GetForwardEntry(0))
			itr.bpm.UnpinPage(prevNodeId, false)
		}
		itr.curIdx = -1
	}

	itr.curIdx++
	if (itr.rangeEndKey != nil && itr.curNode.GetEntry(int(itr.curIdx), itr.keyType).Key.CompareGreaterThan(*itr.rangeEndKey)) ||
		itr.curNode.GetBiggestKey(itr.keyType).IsInfMax() {
		itr.bpm.UnpinPage(itr.curNode.GetPageId(), false)
		return true, nil, nil, math.MaxUint32
	}

	tmpKey := itr.curNode.GetEntry(int(itr.curIdx), itr.keyType).Key
	return false, nil, &tmpKey, itr.curNode.GetEntry(int(itr.curIdx), itr.keyType).Value
}

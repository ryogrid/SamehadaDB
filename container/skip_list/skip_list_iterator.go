package skip_list

import (
	"github.com/ryogrid/SamehadaDB/storage/page/skip_list_page"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
)

type SkipListIterator struct {
	//bpm        *buffer.BufferPoolManager
	//headerPage *skip_list_page.SkipListHeaderPage
	//bucket     uint32
	//offset     uint32
	//blockId    types.PageID
	//blockPage  *skip_list_page.SkipListBlockPage
	curNode       *skip_list_page.SkipListBlockPage
	curEntry      *skip_list_page.SkipListPair
	curIdx        int32
	rangeStartKey *types.Value
	rangeEndKey   *types.Value
}

func (itr *SkipListIterator) Next() (done bool, err error, key *types.Value, val uint32) {
	if itr.curIdx+1 >= itr.curNode.GetEntryCnt() {
		itr.curNode = itr.curNode.GetForwardEntry(0)
		for itr.curNode.GetIsNeedDeleted() && !itr.curNode.GetSmallestKey().IsInfMax() {
			// skip isNeedDeleted marked node
			itr.curNode = itr.curNode.GetForwardEntry(0)
		}
		itr.curIdx = -1
	}

	itr.curIdx++
	if (itr.rangeEndKey != nil && itr.curNode.GetEntry(itr.curIdx).Key.CompareGreaterThan(*itr.rangeEndKey)) ||
		itr.curNode.GetSmallestKey().IsInfMax() {
		return true, nil, nil, math.MaxUint32
	}

	tmpKey := itr.curNode.GetEntry(itr.curIdx).Key
	return false, nil, &tmpKey, itr.curNode.GetEntry(itr.curIdx).Value
}

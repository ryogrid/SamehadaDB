// TODO: (SDB) not implemented yet skip_list_iterator.go

package skip_list

import (
	"github.com/ryogrid/SamehadaDB/storage/page/skip_list_page"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
)

type SkipListIteratorOnMem struct {
	curNode       *SkipListOnMem
	rangeStartKey *types.Value
	rangeEndKey   *types.Value
}

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

func (itr *SkipListIteratorOnMem) Next() (done bool, err error, key *types.Value, val uint32) {
	itr.curNode = itr.curNode.Forward[0]
	if (itr.rangeEndKey != nil && itr.curNode.Key.CompareGreaterThan(*itr.rangeEndKey)) || itr.curNode.Key.IsInfMax() {
		return true, nil, nil, math.MaxUint32
	}

	return false, nil, itr.curNode.Key, itr.curNode.Val
}

func (itr *SkipListIterator) Next() (done bool, err error, key *types.Value, val uint32) {
	if itr.curIdx+1 >= itr.curNode.EntryCnt {
		itr.curNode = itr.curNode.Forward[0]
		itr.curIdx = -1
	}

	itr.curIdx++
	if (itr.rangeEndKey != nil && itr.curNode.Entries[itr.curIdx].Key.CompareGreaterThan(*itr.rangeEndKey)) ||
		itr.curNode.SmallestKey.IsInfMax() {
		return true, nil, nil, math.MaxUint32
	}

	tmpKey := itr.curNode.Entries[itr.curIdx].Key
	return false, nil, &tmpKey, itr.curNode.Entries[itr.curIdx].Value
}

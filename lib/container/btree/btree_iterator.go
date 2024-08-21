package btree

import (
	"github.com/ryogrid/SamehadaDB/lib/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/page/skip_list_page"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

type BTreeIterator struct {
	bltw          *BLTreeWrapper
	bpm           *buffer.BufferPoolManager
	curNode       *skip_list_page.SkipListBlockPage
	curEntry      *skip_list_page.SkipListPair
	rangeStartKey *types.Value
	rangeEndKey   *types.Value
	keyType       types.TypeID
	entryList     []*skip_list_page.SkipListPair
	curEntryIdx   int32
}

func NewSkipListIterator(bltr *BLTreeWrapper, rangeStartKey *types.Value, rangeEndKey *types.Value) *BTreeIterator {
	ret := new(BTreeIterator)

	// TODO: (SDB) need to implement this
	panic("Not implemented yet")
	//headerPage := sl.getHeaderPage()
	//
	//ret.sl = sl
	//ret.bpm = sl.bpm
	//
	//ret.rangeStartKey = rangeStartKey
	//ret.rangeEndKey = rangeEndKey
	//ret.keyType = headerPage.GetKeyType()
	//ret.entryList = make([]*skip_list_page.SkipListPair, 0)
	//
	//ret.initRIDList(sl)

	return ret
}

func (itr *BTreeIterator) initRIDList(bltw *BLTreeWrapper) {
	// TODO: (SDB) need to implement this
	panic("Not implemented yet")
}

func (itr *BTreeIterator) Next() (done bool, err error, key *types.Value, rid *page.RID) {
	// TODO: (SDB) need to implement this
	panic("Not implemented yet")
	if itr.curEntryIdx < int32(len(itr.entryList)) {
		ret := itr.entryList[itr.curEntryIdx]
		itr.curEntryIdx++
		tmpRID := samehada_util.UnpackUint64toRID(ret.Value)
		return false, nil, samehada_util.GetPonterOfValue(ret.Key), &tmpRID
	} else {
		return true, nil, nil, nil
	}
}

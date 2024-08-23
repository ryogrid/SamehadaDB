package btree

import (
	"github.com/ryogrid/SamehadaDB/lib/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_common"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/page/skip_list_page"
	"github.com/ryogrid/SamehadaDB/lib/types"
	blink_tree "github.com/ryogrid/bltree-go-for-embedding"
)

type BTreeIterator struct {
	bltr          *blink_tree.BLTree
	bpm           *buffer.BufferPoolManager
	curNode       *skip_list_page.SkipListBlockPage
	curEntry      *index_common.IndexEntry
	rangeStartKey *types.Value
	rangeEndKey   *types.Value
	keyType       types.TypeID
	entryList     []*index_common.IndexEntry
	curEntryIdx   int32
}

func NewSkipListIterator(bltr *blink_tree.BLTree, rangeStartKey *types.Value, rangeEndKey *types.Value) *BTreeIterator {
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
	//ret.entryList = make([]*skip_list_page.IndexEntry, 0)
	//
	//ret.initRIDList(sl)

	return ret
}

func (itr *BTreeIterator) initRIDList(bltr *blink_tree.BLTree) {
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

package skip_list_page

import (
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"unsafe"
)

/**
 *
 * Header Page for Skip list.
 * (Header Page is placed page memory area. so serialization/desirialization of each member is not needed)
 *
 * page format (size in byte, 12 bytes in total):
 * ------------------------------------------------------------------
 * | pageID (4) | listStartPageId (4) | curMaxLevel (4) | keyType (4) |
 * -----------------------------------------------------------------
 */

const (
	MAX_FOWARD_LIST_LEN = 20
)

type SkipListHeaderPage struct {
	//page.Page
	// Header's successor node has all level path
	// and header does'nt have no entry

	pageId          types.PageID
	listStartPageId types.PageID //*SkipListBlockPage
	curMaxLevel     int32
	keyType         types.TypeID // used when load list datas from disk
}

func NewSkipListStartBlockPage(bpm *buffer.BufferPoolManager, keyType types.TypeID) types.PageID {
	//startPage.ID()
	var startNode *SkipListBlockPage = nil
	switch keyType {
	case types.Integer:
		startNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, SkipListPair{types.NewInteger(math.MinInt32), 0})
	case types.Float:
		startNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, SkipListPair{types.NewFloat(math.SmallestNonzeroFloat32), 0})
	case types.Varchar:
		startNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, SkipListPair{types.NewVarchar(""), 0})
	case types.Boolean:
		startNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, SkipListPair{types.NewBoolean(false), 0})
	}

	var sentinelNode *SkipListBlockPage = nil
	switch keyType {
	case types.Integer:
		pl := SkipListPair{types.NewInteger(0), 0}
		pl.Key = *pl.Key.SetInfMax()
		sentinelNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, pl)
	case types.Float:
		pl := SkipListPair{types.NewFloat(0), 0}
		pl.Key = *pl.Key.SetInfMax()
		sentinelNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, pl)
	case types.Varchar:
		pl := SkipListPair{types.NewVarchar(""), 0}
		pl.Key = *pl.Key.SetInfMax()
		sentinelNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, pl)
	case types.Boolean:
		pl := SkipListPair{types.NewBoolean(false), 0}
		pl.Key = *pl.Key.SetInfMax()
		sentinelNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, pl)
	}

	startNode.SetLevel(1)
	//startNode.SetForward(make([]*SkipListBlockPage, MAX_FOWARD_LIST_LEN))
	// set sentinel node at end of list
	for ii := 0; ii < MAX_FOWARD_LIST_LEN; ii++ {
		startNode.SetForwardEntry(ii, sentinelNode.GetPageId())
	}

	ret := startNode.GetPageId()
	bpm.UnpinPage(startNode.GetPageId(), true)
	bpm.UnpinPage(sentinelNode.GetPageId(), true)

	return ret
}

func (hp *SkipListHeaderPage) SetPageId(pageId types.PageID) {
	hp.pageId = pageId
}

func (hp *SkipListHeaderPage) GetPageId() types.PageID {
	return hp.pageId
}

func (hp *SkipListHeaderPage) GetListStartPageId() types.PageID {
	return hp.listStartPageId
	//return nil
}

func (hp *SkipListHeaderPage) SetListStartPageId(bpId types.PageID) {
	hp.listStartPageId = bpId
}

func (hp *SkipListHeaderPage) GetCurMaxLevel() int32 {
	return hp.curMaxLevel
	//return -1
}

func (hp *SkipListHeaderPage) SetCurMaxLevel(maxLevel int32) {
	if maxLevel < 1 {
		panic("SetCurMaxLevel: invalid maxLevel is passed!")
	}
	hp.curMaxLevel = maxLevel
}

func (hp *SkipListHeaderPage) GetKeyType() types.TypeID {
	return hp.keyType
	//return types.TypeID(-1)
}

func (hp *SkipListHeaderPage) SetKeyType(ktype types.TypeID) {
	hp.keyType = ktype
}

func NewSkipListHeaderPage(bpm *buffer.BufferPoolManager, keyType types.TypeID) types.PageID {
	page_ := bpm.NewPage()
	headerData := page_.Data()
	headerPage := (*SkipListHeaderPage)(unsafe.Pointer(headerData))
	headerPage.SetPageId(page_.GetPageId())

	headerPage.SetListStartPageId(NewSkipListStartBlockPage(bpm, keyType))
	headerPage.SetCurMaxLevel(1)
	headerPage.SetKeyType(keyType)

	retPageID := headerPage.GetPageId()
	bpm.UnpinPage(headerPage.GetPageId(), true)

	return retPageID
}

// TODO: (SDB) in concurrent impl, locking in this method is needed. and caller must do unlock (FectchAndCastToBlockPage)

// Attention:
//   caller must call UnpinPage with appropriate diaty page to the got page when page using ends
func FetchAndCastToHeaderPage(bpm *buffer.BufferPoolManager, pageId types.PageID) *SkipListHeaderPage {
	hPageData := bpm.FetchPage(pageId).Data()
	return (*SkipListHeaderPage)(unsafe.Pointer(hPageData))
}

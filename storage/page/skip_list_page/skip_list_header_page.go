package skip_list_page

import (
	"bytes"
	"encoding/binary"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"unsafe"
)

const (
	hOffsetPageId     = 0
	offsetStartPageId = page.OffsetLSN + types.SizeOfLSN
	offsetKeyType     = offsetStartPageId + sizeStartPageId
	hSizePageId       = 4
	sizeStartPageId   = 4
	sizeKeyType       = 4
)

/**
 *
 * Header Page for Skip list.
 * (Header Page is placed page memory area. so serialization/desirialization of each member is not needed)
 *
 * page format (size in byte, 12 bytes in total):
 * -----------------------------------------------------------
 * | pageID (4) | LSN(4) | listStartPageId (4) | keyType (4) |
 * ----------------------------------------------------------
 */

const (
	MAX_FOWARD_LIST_LEN = 20
)

type SkipListHeaderPage struct {
	page.Page
	// Header's successor node has all level path

	//pageId          types.PageID
	//pageLSN         types.LSN
	//listStartPageId types.PageID //*SkipListBlockPage
	//keyType         types.TypeID // used when load list datas from disk
}

func NewSkipListStartBlockPage(bpm *buffer.BufferPoolManager, keyType types.TypeID) (startNode_ *SkipListBlockPage, sentinelNode_ *SkipListBlockPage) {
	//startPage.GetPageId()
	var startNode *SkipListBlockPage = nil
	switch keyType {
	case types.Integer:
		startNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, SkipListPair{types.NewInteger(math.MinInt32), 0})
	case types.Float:
		startNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, SkipListPair{types.NewFloat(-1.0 * math.MaxFloat32), 0})
	case types.Varchar:
		v := types.NewVarchar("")
		v.SetInfMin()
		startNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, SkipListPair{v, 0})
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

	// set sentinel node at end of list
	for ii := 0; ii < MAX_FOWARD_LIST_LEN; ii++ {
		startNode.SetForwardEntry(ii, sentinelNode.GetPageId())
	}

	//ret := startNode.GetPageId()
	//bpm.UnpinPage(startNode.GetPageId(), true)
	//bpm.UnpinPage(sentinelNode.GetPageId(), true)

	return startNode, sentinelNode
}

func (hp *SkipListHeaderPage) GetPageId() types.PageID {
	return types.PageID(types.NewInt32FromBytes(hp.Data()[hOffsetPageId:]))
}

func (hp *SkipListHeaderPage) SetPageId(pageId types.PageID) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, pageId)
	pageIdInBytes := buf.Bytes()
	copy(hp.Data()[hOffsetPageId:], pageIdInBytes)
}

func (hp *SkipListHeaderPage) GetListStartPageId() types.PageID {
	return types.PageID(types.NewInt32FromBytes(hp.Data()[offsetStartPageId:]))
}

func (hp *SkipListHeaderPage) SetListStartPageId(pageId types.PageID) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, pageId)
	startPageIdInBytes := buf.Bytes()
	copy(hp.Data()[offsetStartPageId:], startPageIdInBytes)
}

func (hp *SkipListHeaderPage) GetKeyType() types.TypeID {
	return types.TypeID(types.NewInt32FromBytes(hp.Data()[offsetKeyType:]))
}

func (hp *SkipListHeaderPage) SetKeyType(ktype types.TypeID) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(ktype))
	keyTypeInBytes := buf.Bytes()
	copy(hp.Data()[offsetKeyType:], keyTypeInBytes)
}

func NewSkipListHeaderPage(bpm *buffer.BufferPoolManager, keyType types.TypeID) (headerPage_ *SkipListHeaderPage, startNode_ *SkipListBlockPage, sentinelNode_ *SkipListBlockPage) {
	page_ := bpm.NewPage()
	headerPage := (*SkipListHeaderPage)(unsafe.Pointer(page_))
	headerPage.SetPageId(page_.GetPageId())
	headerPage.Page.SetLSN(0)
	startNode, sentinelNode := NewSkipListStartBlockPage(bpm, keyType)
	headerPage.SetListStartPageId(startNode.GetPageId())
	headerPage.SetKeyType(keyType)

	//retPageID := headerPage.GetPageId()
	//bpm.UnpinPage(headerPage.GetPageId(), true)

	//return retPageID
	return headerPage, startNode, sentinelNode
}

// Attention:
//
//	caller must call UnpinPage with appropriate diaty page to the got page when page using ends
func FetchAndCastToHeaderPage(bpm *buffer.BufferPoolManager, pageId types.PageID) *SkipListHeaderPage {
	page_ := bpm.FetchPage(pageId)
	hpage := (*SkipListHeaderPage)(unsafe.Pointer(page_))
	return hpage
}

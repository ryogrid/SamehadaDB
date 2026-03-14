package skip_list_page

import (
	"bytes"
	"encoding/binary"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_common"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"math"
	"unsafe"
)

const (
	hOffsetPageID     = 0
	offsetStartPageID = page.OffsetLSN + types.SizeOfLSN
	offsetKeyType     = offsetStartPageID + sizeStartPageID
	hSizePageID       = 4
	sizeStartPageID   = 4
	sizeKeyType       = 4
)

/**
 *
 * Header Page for Skip list.
 * (Header Page is placed page memory area. so serialization/desirialization of each member is not needed)
 *
 * page format (size in byte, 12 bytes in total):
 * -----------------------------------------------------------
 * | pageID (4) | LSN(4) | listStartPageID (4) | keyType (4) |
 * ----------------------------------------------------------
 */

const (
	MaxForwardListLen = 20
)

type SkipListHeaderPage struct {
	page.Page
	// Header's successor node has all level path
}

func NewSkipListStartBlockPage(bpm *buffer.BufferPoolManager, keyType types.TypeID) (startNodeRet *SkipListBlockPage, sentinelNodeRet *SkipListBlockPage) {
	var startNode *SkipListBlockPage = nil
	switch keyType {
	case types.Integer:
		startNode = NewSkipListBlockPage(bpm, MaxForwardListLen, index_common.IndexEntry{types.NewInteger(math.MinInt32), 0})
	case types.Float:
		startNode = NewSkipListBlockPage(bpm, MaxForwardListLen, index_common.IndexEntry{types.NewFloat(-1.0 * math.MaxFloat32), 0})
	case types.Varchar:
		v := types.NewVarchar("")
		v.SetInfMin()
		startNode = NewSkipListBlockPage(bpm, MaxForwardListLen, index_common.IndexEntry{v, 0})
	case types.Boolean:
		startNode = NewSkipListBlockPage(bpm, MaxForwardListLen, index_common.IndexEntry{types.NewBoolean(false), 0})
	}

	var sentinelNode *SkipListBlockPage = nil
	switch keyType {
	case types.Integer:
		pl := index_common.IndexEntry{types.NewInteger(0), 0}
		pl.Key = *pl.Key.SetInfMax()
		sentinelNode = NewSkipListBlockPage(bpm, MaxForwardListLen, pl)
	case types.Float:
		pl := index_common.IndexEntry{types.NewFloat(0), 0}
		pl.Key = *pl.Key.SetInfMax()
		sentinelNode = NewSkipListBlockPage(bpm, MaxForwardListLen, pl)
	case types.Varchar:
		pl := index_common.IndexEntry{types.NewVarchar(""), 0}
		pl.Key = *pl.Key.SetInfMax()
		sentinelNode = NewSkipListBlockPage(bpm, MaxForwardListLen, pl)
	case types.Boolean:
		pl := index_common.IndexEntry{types.NewBoolean(false), 0}
		pl.Key = *pl.Key.SetInfMax()
		sentinelNode = NewSkipListBlockPage(bpm, MaxForwardListLen, pl)
	}

	startNode.SetLevel(1)

	// set sentinel node at end of list
	for ii := 0; ii < MaxForwardListLen; ii++ {
		startNode.SetForwardEntry(ii, sentinelNode.GetPageID())
	}

	return startNode, sentinelNode
}

func (hp *SkipListHeaderPage) GetPageID() types.PageID {
	return types.PageID(types.NewInt32FromBytes(hp.Data()[hOffsetPageID:]))
}

func (hp *SkipListHeaderPage) SetPageID(pageID types.PageID) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, pageID)
	pageIDInBytes := buf.Bytes()
	copy(hp.Data()[hOffsetPageID:], pageIDInBytes)
}

func (hp *SkipListHeaderPage) GetListStartPageID() types.PageID {
	return types.PageID(types.NewInt32FromBytes(hp.Data()[offsetStartPageID:]))
}

func (hp *SkipListHeaderPage) SetListStartPageID(pageID types.PageID) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, pageID)
	startPageIDInBytes := buf.Bytes()
	copy(hp.Data()[offsetStartPageID:], startPageIDInBytes)
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

func NewSkipListHeaderPage(bpm *buffer.BufferPoolManager, keyType types.TypeID) (headerPageRet *SkipListHeaderPage, startNodeRet *SkipListBlockPage, sentinelNodeRet *SkipListBlockPage) {
	pg := bpm.NewPage()
	headerPage := (*SkipListHeaderPage)(unsafe.Pointer(pg))
	headerPage.SetPageID(pg.GetPageID())
	headerPage.Page.SetLSN(0)
	startNode, sentinelNode := NewSkipListStartBlockPage(bpm, keyType)
	headerPage.SetListStartPageID(startNode.GetPageID())
	headerPage.SetKeyType(keyType)

	return headerPage, startNode, sentinelNode
}

// Attention:
//
//	caller must call UnpinPage with appropriate diaty page to the got page when page using ends
func FetchAndCastToHeaderPage(bpm *buffer.BufferPoolManager, pageID types.PageID) *SkipListHeaderPage {
	pg := bpm.FetchPage(pageID)
	hpage := (*SkipListHeaderPage)(unsafe.Pointer(pg))
	return hpage
}

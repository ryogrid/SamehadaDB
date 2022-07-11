package skip_list_page

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"unsafe"
)

/**
 *
 * Header Page for linear probing hash table.
 *
 * Header format (size in byte, 16 bytes in total):
 * -------------------------------------------------------------
 * | LSN (4) | Size (4) | PageId(4) | NextBlockIndex(4)
 * -------------------------------------------------------------
 */

const (
	MAX_FOWARD_LIST_LEN = 20
)

type SkipListPair struct {
	Key   types.Value
	Value uint32
}

type SkipListHeaderPageOnMem struct {
	pageId       types.PageID
	lsn          int    // log sequence number
	nextIndex    uint32 // the next index to add a new entry to blockPageIds
	size         int    // the number of key/value pairs the hash table can hold
	blockPageIds [1020]types.PageID
}

type SkipListHeaderPage struct {
	//page.Page
	// Header's successor node has all level path
	// and header does'nt have no entry
	ListStartPage *SkipListBlockPage //types.PageID
	CurMaxLevel   int32
	KeyType       types.TypeID // used when load list datas from disk
	rwlatch_      common.ReaderWriterLatch
}

func NewSkipListStartBlockPage(bpm *buffer.BufferPoolManager, keyType types.TypeID) *SkipListBlockPage {
	//startPage.ID()
	var startNode *SkipListBlockPage = nil
	switch keyType {
	case types.Integer:
		startNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, &SkipListPair{types.NewInteger(math.MinInt32), 0})
	case types.Float:
		startNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, &SkipListPair{types.NewFloat(math.SmallestNonzeroFloat32), 0})
	case types.Varchar:
		startNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, &SkipListPair{types.NewVarchar(""), 0})
	case types.Boolean:
		startNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, &SkipListPair{types.NewBoolean(false), 0})
	}

	var sentinelNode *SkipListBlockPage = nil
	switch keyType {
	case types.Integer:
		pl := &SkipListPair{types.NewInteger(0), 0}
		pl.Key.SetInfMax()
		sentinelNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, pl)
	case types.Float:
		pl := &SkipListPair{types.NewFloat(0), 0}
		pl.Key.SetInfMax()
		sentinelNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, pl)
	case types.Varchar:
		pl := &SkipListPair{types.NewVarchar(""), 0}
		pl.Key.SetInfMax()
		sentinelNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, pl)
	case types.Boolean:
		pl := &SkipListPair{types.NewBoolean(false), 0}
		pl.Key.SetInfMax()
		sentinelNode = NewSkipListBlockPage(bpm, MAX_FOWARD_LIST_LEN, pl)
	}

	// TODO: (SDB) contents of startNode is broken here!

	startNode.Level = 1
	startNode.Forward = make([]*SkipListBlockPage, MAX_FOWARD_LIST_LEN)
	// set sentinel node at end of list
	for ii := 0; ii < MAX_FOWARD_LIST_LEN; ii++ {
		startNode.Forward[ii] = sentinelNode
	}

	return startNode
}

func NewSkipListHeaderPage(bpm *buffer.BufferPoolManager, keyType types.TypeID) *SkipListHeaderPage {
	page_ := bpm.NewPage()
	headerData := page_.Data()
	headerPage := (*SkipListHeaderPage)(unsafe.Pointer(headerData))

	headerPage.ListStartPage = NewSkipListStartBlockPage(bpm, keyType)
	headerPage.CurMaxLevel = 1

	return headerPage
}

// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package disk

import (
	"github.com/ryogrid/SamehadaDB/lib/types"
)

/**
 * DiskManager takes care of the allocation and deallocation of pages within a database. It performs the reading and
 * writing of pages to and from disk, providing a logical file layer within the context of a database management system.
 */
type DiskManager interface {
	ReadPage(types.PageID, []byte) error
	WritePage(types.PageID, []byte) error
	AllocatePage() types.PageID
	DeallocatePage(types.PageID)
	GetNumWrites() uint64
	ShutDown()
	Size() int64
	RemoveDBFile()
	RemoveLogFile()
	WriteLog([]byte) error
	ReadLog([]byte, int32, *uint32) bool
	GetLogFileSize() int64
	GCLogFile() error
}

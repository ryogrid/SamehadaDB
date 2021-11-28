// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package disk

import (
	"github.com/ryogrid/SaitomDB/types"
)

// DiskManager is responsible for interacting with disk
type DiskManager interface {
	ReadPage(types.PageID, []byte) error
	WritePage(types.PageID, []byte) error
	AllocatePage() types.PageID
	DeallocatePage(types.PageID)
	GetNumWrites() uint64
	ShutDown()
	Size() int64
}

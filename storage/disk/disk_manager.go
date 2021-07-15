package disk

import (
	"github.com/brunocalza/go-bustub/types"
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

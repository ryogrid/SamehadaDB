package disk

import (
	"github.com/brunocalza/go-bustub/storage/page"
)

// DiskManager is responsible for interacting with disk
type DiskManager interface {
	ReadPage(page.PageID, []byte) error
	WritePage(page.PageID, []byte) error
	AllocatePage() page.PageID
	DeallocatePage(page.PageID)
	GetNumWrites() uint64
	ShutDown()
}

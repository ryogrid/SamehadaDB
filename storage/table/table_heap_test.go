package table

import (
	"testing"

	"github.com/brunocalza/go-bustub/buffer"
	"github.com/brunocalza/go-bustub/storage/disk"
)

func TestTableHeap(t *testing.T) {
	diskManager := disk.NewDiskManagerImpl("test.db")
	bpm := buffer.NewBufferPoolManager(diskManager, buffer.NewClockReplacer(10))

	NewTableHeap(bpm)
}

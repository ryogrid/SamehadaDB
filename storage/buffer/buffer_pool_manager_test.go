package buffer

import (
	"crypto/rand"
	"testing"

	"github.com/brunocalza/go-bustub/storage/disk"
	"github.com/brunocalza/go-bustub/storage/page"
	testingpkg "github.com/brunocalza/go-bustub/testing"
	"github.com/brunocalza/go-bustub/types"
)

func TestBinaryData(t *testing.T) {
	poolSize := uint32(10)

	dm := disk.NewDiskManagerTest()
	defer dm.ShutDown()
	bpm := NewBufferPoolManager(poolSize, dm)

	page0 := bpm.NewPage()

	// Scenario: The buffer pool is empty. We should be able to create a new page.
	testingpkg.Equals(t, types.PageID(0), page0.ID())

	// Generate random binary data
	randomBinaryData := make([]byte, page.PageSize)
	rand.Read(randomBinaryData)

	// Insert terminal characters both in the middle and at end
	randomBinaryData[page.PageSize/2] = '0'
	randomBinaryData[page.PageSize-1] = '0'

	var fixedRandomBinaryData [page.PageSize]byte
	copy(fixedRandomBinaryData[:], randomBinaryData[:page.PageSize])

	// Scenario: Once we have a page, we should be able to read and write content.
	page0.Copy(0, randomBinaryData)
	testingpkg.Equals(t, fixedRandomBinaryData, *page0.Data())

	// Scenario: We should be able to create new pages until we fill up the buffer pool.
	for i := uint32(1); i < poolSize; i++ {
		p := bpm.NewPage()
		testingpkg.Equals(t, types.PageID(i), p.ID())
	}

	// Scenario: Once the buffer pool is full, we should not be able to create any new pages.
	for i := poolSize; i < poolSize*2; i++ {
		testingpkg.Equals(t, (*page.Page)(nil), bpm.NewPage())
	}

	// Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
	// there would still be one cache frame left for reading page 0.
	for i := 0; i < 5; i++ {
		testingpkg.Ok(t, bpm.UnpinPage(types.PageID(i), true))
		bpm.FlushPage(types.PageID(i))
	}
	for i := 0; i < 4; i++ {
		p := bpm.NewPage()
		bpm.UnpinPage(p.ID(), false)
	}

	// Scenario: We should be able to fetch the data we wrote a while ago.
	page0 = bpm.FetchPage(types.PageID(0))
	testingpkg.Equals(t, fixedRandomBinaryData, *page0.Data())
	testingpkg.Ok(t, bpm.UnpinPage(types.PageID(0), true))
}

func TestSample(t *testing.T) {
	poolSize := uint32(10)

	dm := disk.NewDiskManagerTest()
	defer dm.ShutDown()
	bpm := NewBufferPoolManager(poolSize, dm)

	page0 := bpm.NewPage()

	// Scenario: The buffer pool is empty. We should be able to create a new page.
	testingpkg.Equals(t, types.PageID(0), page0.ID())

	// Scenario: Once we have a page, we should be able to read and write content.
	page0.Copy(0, []byte("Hello"))
	testingpkg.Equals(t, [page.PageSize]byte{'H', 'e', 'l', 'l', 'o'}, *page0.Data())

	// Scenario: We should be able to create new pages until we fill up the buffer pool.
	for i := uint32(1); i < poolSize; i++ {
		p := bpm.NewPage()
		testingpkg.Equals(t, types.PageID(i), p.ID())
	}

	// Scenario: Once the buffer pool is full, we should not be able to create any new pages.
	for i := poolSize; i < poolSize*2; i++ {
		testingpkg.Equals(t, (*page.Page)(nil), bpm.NewPage())
	}

	// Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
	// there would still be one cache frame left for reading page 0.
	for i := 0; i < 5; i++ {
		testingpkg.Ok(t, bpm.UnpinPage(types.PageID(i), true))
		bpm.FlushPage(types.PageID(i))
	}
	for i := 0; i < 4; i++ {
		bpm.NewPage()
	}
	// Scenario: We should be able to fetch the data we wrote a while ago.
	page0 = bpm.FetchPage(types.PageID(0))
	testingpkg.Equals(t, [page.PageSize]byte{'H', 'e', 'l', 'l', 'o'}, *page0.Data())

	// Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
	// now be pinned. Fetching page 0 should fail.
	testingpkg.Ok(t, bpm.UnpinPage(types.PageID(0), true))

	testingpkg.Equals(t, types.PageID(14), bpm.NewPage().ID())
	testingpkg.Equals(t, (*page.Page)(nil), bpm.NewPage())
	testingpkg.Equals(t, (*page.Page)(nil), bpm.FetchPage(types.PageID(0)))
}

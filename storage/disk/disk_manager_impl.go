package disk

import (
	"errors"
	"io"
	"log"
	"os"

	"github.com/brunocalza/go-bustub/storage/page"
)

//DiskManagerImpl is the disk implementation of DiskManager
type DiskManagerImpl struct {
	db         *os.File
	fileName   string
	nextPageID page.PageID
	numWrites  uint64
}

// NewDiskManagerImpl returns a DiskManager instance
func NewDiskManagerImpl(dbFilename string) DiskManager {
	d := &DiskManagerImpl{
		fileName:   dbFilename,
		nextPageID: 0,
		numWrites:  0,
	}

	var err error
	d.db, err = os.OpenFile(dbFilename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalln("can't open db file")
	}

	return d
}

// ShutDown closes of the database file
func (d *DiskManagerImpl) ShutDown() {
	d.db.Close()
}

// Write a page to the database file
func (d *DiskManagerImpl) WritePage(pageId page.PageID, pageData []byte) error {
	offset := int64(pageId * page.PageSize)
	d.numWrites++
	d.db.Seek(offset, io.SeekStart)
	bytesWritten, err := d.db.Write(pageData)
	if err != nil {
		return err
	}

	if bytesWritten != page.PageSize {
		return errors.New("bytes written not equals page size")
	}

	d.db.Sync()
	return nil
}

// Read a page from the database file
func (d *DiskManagerImpl) ReadPage(pageID page.PageID, pageData []byte) error {
	offset := int64(pageID * page.PageSize)

	fileInfo, err := d.db.Stat()
	if err != nil {
		return errors.New("file info error")
	}

	if offset > fileInfo.Size() {
		return errors.New("I/O error past end of file")
	}

	d.db.Seek(offset, io.SeekStart)

	bytesRead, err := d.db.Read(pageData)
	if err != nil {
		return errors.New("I/O error while reading")
	}

	if bytesRead < page.PageSize {
		for i := 0; i < page.PageSize; i++ {
			pageData[i] = 0
		}
	}
	return nil
}

//  AllocatePage allocates a new page
//  For now just keep an increasing counter
func (d *DiskManagerImpl) AllocatePage() page.PageID {
	ret := d.nextPageID
	d.nextPageID++
	return ret
}

// DeallocatePage deallocates page
// Need bitmap in header page for tracking pages
// This does not actually need to do anything for now.
func (d *DiskManagerImpl) DeallocatePage(pageID page.PageID) {
}

// GetNumWrites returns the number of disk writes
func (d *DiskManagerImpl) GetNumWrites() uint64 {
	return d.numWrites
}

// Size returns the size of the file in disk
func (d *DiskManagerImpl) Size() (int64, error) {
	fileInfo, err := d.db.Stat()
	if err != nil {
		return 0, errors.New("file info error")
	}

	return fileInfo.Size(), nil
}

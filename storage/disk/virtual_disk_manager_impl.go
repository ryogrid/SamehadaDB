package disk

import (
	"errors"
	"strings"
	"sync"

	"github.com/dsnet/golib/memfile"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/types"
)

// DiskManagerImpl is the disk implementation of DiskManager
type VirtualDiskManagerImpl struct {
	db           *memfile.File //[]byte
	fileName     string
	log          *memfile.File //[]byte
	fileName_log string
	nextPageID   types.PageID
	numWrites    uint64
	size         int64
	flush_log    bool
	numFlushes   uint64
	dbFileMutex  *sync.Mutex
	logFileMutex *sync.Mutex
}

func NewVirtualDiskManagerImpl(dbFilename string) DiskManager {
	file := memfile.New(make([]byte, 0))

	period_idx := strings.LastIndex(dbFilename, ".")
	logfname_base := dbFilename[:period_idx]
	logfname := logfname_base + "." + "log"

	file_1 := memfile.New(make([]byte, 0))

	fileSize := int64(0)
	nextPageID := types.PageID(0)

	return &VirtualDiskManagerImpl{file, dbFilename, file_1, logfname, nextPageID, 0, fileSize, false, 0, new(sync.Mutex), new(sync.Mutex)}
}

// ShutDown closes of the database file
func (d *VirtualDiskManagerImpl) ShutDown() {
	// do nothing
}

// Write a page to the database file
func (d *VirtualDiskManagerImpl) WritePage(pageId types.PageID, pageData []byte) error {
	d.dbFileMutex.Lock()
	defer d.dbFileMutex.Unlock()

	offset := int64(pageId * common.PageSize)
	d.db.WriteAt(pageData, offset)

	if offset >= d.size {
		d.size = offset + int64(len(pageData))
	}

	return nil
}

// Read a page from the database file
func (d *VirtualDiskManagerImpl) ReadPage(pageID types.PageID, pageData []byte) error {
	d.dbFileMutex.Lock()
	defer d.dbFileMutex.Unlock()

	offset := int64(pageID * common.PageSize)

	currentSize := int64(len(d.db.Bytes()))
	if offset > currentSize || offset+int64(len(pageData)) > currentSize {
		return errors.New("I/O error past end of file")
	}

	d.db.ReadAt(pageData, offset)
	return nil
}

// AllocatePage allocates a new page
func (d *VirtualDiskManagerImpl) AllocatePage() types.PageID {
	d.dbFileMutex.Lock()

	ret := d.nextPageID

	//// extend db file for avoiding later ReadPage and WritePage fails
	//zeroClearedPageData := make([]byte, common.PageSize)
	//
	//d.dbFileMutex.Unlock()
	//d.WritePage(ret, zeroClearedPageData)
	//d.dbFileMutex.Lock()
	defer d.dbFileMutex.Unlock()

	d.nextPageID++
	return ret
}

// DeallocatePage deallocates page
// Need bitmap in header page for tracking pages
// This does not actually need to do anything for now.
func (d *VirtualDiskManagerImpl) DeallocatePage(pageID types.PageID) {}

// GetNumWrites returns the number of disk writes
func (d *VirtualDiskManagerImpl) GetNumWrites() uint64 {
	return d.numWrites
}

// Size returns the size of the file in disk
func (d *VirtualDiskManagerImpl) Size() int64 {
	d.dbFileMutex.Lock()
	defer d.dbFileMutex.Unlock()
	return d.size
}

// ATTENTION: this method can be call after calling of Shutdown method
func (d *VirtualDiskManagerImpl) RemoveDBFile() {
	// do nothing
}

// ATTENTION: this method can be call after calling of Shutdown method
func (d *VirtualDiskManagerImpl) RemoveLogFile() {
	// do nothing
}

// erase needless data from log file (use this when db recovery finishes or snapshot finishes)
// file content becomes empty
func (d *VirtualDiskManagerImpl) GCLogFile() error {
	d.logFileMutex.Lock()

	d.log = memfile.New(make([]byte, 0))
	d.logFileMutex.Lock()
	defer d.logFileMutex.Unlock()

	return nil
}

/**
 * Write the contents of the log into disk file
 * Only return when sync is done, and only perform sequence write
 */
func (d *VirtualDiskManagerImpl) WriteLog(log_data []byte) {
	d.logFileMutex.Lock()
	defer d.logFileMutex.Unlock()

	d.flush_log = true

	d.numFlushes += 1
	// sequence write
	d.log.Write(log_data)
	d.flush_log = false
}

/**
* Read the contents of the log into the given memory area
* Always read from the beginning and perform sequence read
* @return: false means already reach the end
 */
// Attention: len(log_data) specifies read data length
func (d *VirtualDiskManagerImpl) ReadLog(log_data []byte, offset int32, retReadBytes *uint32) bool {
	if int64(offset) >= d.GetLogFileSize() {
		return false
	}

	d.logFileMutex.Lock()
	defer d.logFileMutex.Unlock()

	readLen := int32(len(log_data))
	d.log.ReadAt(log_data, int64(offset))
	*retReadBytes = uint32(readLen)

	return true
}

/**
 * Private helper function to get disk file size
 */
func (d *VirtualDiskManagerImpl) GetLogFileSize() int64 {
	d.logFileMutex.Lock()
	defer d.logFileMutex.Unlock()

	return int64(len(d.log.Bytes()))
}

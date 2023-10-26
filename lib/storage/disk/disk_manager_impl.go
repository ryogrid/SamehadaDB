// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package disk

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

// DiskManagerImpl is the disk implementation of DiskManager
type DiskManagerImpl struct {
	db           *os.File
	fileName     string
	log          *os.File
	fileName_log string
	nextPageID   types.PageID
	numWrites    uint64
	size         int64
	flush_log    bool
	numFlushes   uint64
	dbFileMutex  *sync.Mutex
	logFileMutex *sync.Mutex
}

// NewDiskManagerImpl returns a DiskManager instance
func NewDiskManagerImpl(dbFilename string) DiskManager {
	file, err := os.OpenFile(dbFilename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalln("can't open db file")
		return nil
	}

	period_idx := strings.LastIndex(dbFilename, ".")
	logfname_base := dbFilename[:period_idx]
	logfname := logfname_base + "." + "log"
	file_1, err := os.OpenFile(logfname, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalln("can't open log file")
		return nil
	}

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalln("file info error")
		return nil
	}

	fileInfo_1, err := file_1.Stat()
	if err != nil {
		log.Fatalln("file info error (log file)")
		return nil
	}

	file_1.Seek(fileInfo_1.Size(), io.SeekStart)

	fileSize := fileInfo.Size()
	nPages := fileSize / common.PageSize

	nextPageID := types.PageID(0)
	if nPages > 0 {
		nextPageID = types.PageID(int32(nPages + 1))
	}

	return &DiskManagerImpl{file, dbFilename, file_1, logfname, nextPageID, 0, fileSize, false, 0, new(sync.Mutex), new(sync.Mutex)}
}

// ShutDown closes of the database file
func (d *DiskManagerImpl) ShutDown() {
	d.dbFileMutex.Lock()
	err := d.db.Close()
	if err != nil {
		fmt.Println(err)
		panic("close of db file failed")
	}
	d.dbFileMutex.Unlock()
	d.logFileMutex.Lock()
	err = d.log.Close()
	if err != nil {
		fmt.Println(err)
		panic("close of log file failed")
	}
	d.logFileMutex.Unlock()
}

// Write a page to the database file
func (d *DiskManagerImpl) WritePage(pageId types.PageID, pageData []byte) error {
	d.dbFileMutex.Lock()
	defer d.dbFileMutex.Unlock()

	offset := int64(pageId) * int64(common.PageSize)
	_, errSeek := d.db.Seek(offset, io.SeekStart)
	if errSeek != nil {
		fmt.Println(errSeek)
		// TODO: (SDB) SHOULD BE FIXED: checkpoint thread's call causes this error rarely
		fmt.Println("WritePge: d.db.Write returns err!")
		return errSeek
	}
	bytesWritten, errWrite := d.db.Write(pageData)
	if errWrite != nil {
		fmt.Println(errWrite)
		panic("WritePge: d.db.Write returns err!")
	}

	if bytesWritten != common.PageSize {
		panic("bytes written not equals page size")
	}

	if offset >= d.size {
		d.size = offset + int64(bytesWritten)
	}

	d.db.Sync()
	return nil
}

// Read a page from the database file
func (d *DiskManagerImpl) ReadPage(pageID types.PageID, pageData []byte) error {
	d.dbFileMutex.Lock()
	defer d.dbFileMutex.Unlock()

	offset := int64(pageID) * int64(common.PageSize)

	fileInfo, err := d.db.Stat()
	if err != nil {
		fmt.Println(err)
		// TODO: (SDB) SHOULD BE FIXED: checkpoint and statics data update thread's call causes this error rarely
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

	if bytesRead < common.PageSize {
		for i := 0; i < common.PageSize; i++ {
			pageData[i] = 0
		}
	}
	return nil
}

// AllocatePage allocates a new page
func (d *DiskManagerImpl) AllocatePage() types.PageID {
	d.dbFileMutex.Lock()

	ret := d.nextPageID
	defer d.dbFileMutex.Unlock()

	d.nextPageID++
	return ret
}

// DeallocatePage deallocates page
// Need bitmap in header page for tracking pages
// This does not actually need to do anything for now.
func (d *DiskManagerImpl) DeallocatePage(pageID types.PageID) {}

// GetNumWrites returns the number of disk writes
func (d *DiskManagerImpl) GetNumWrites() uint64 {
	return d.numWrites
}

// Size returns the size of the file in disk
func (d *DiskManagerImpl) Size() int64 {
	d.dbFileMutex.Lock()
	defer d.dbFileMutex.Unlock()
	return d.size
}

// ATTENTION: this method can be call after calling of Shutdown method
func (d *DiskManagerImpl) RemoveDBFile() {
	d.dbFileMutex.Lock()
	defer d.dbFileMutex.Unlock()

	err := os.Remove(d.fileName)
	if err != nil {
		fmt.Println(err)
		panic("file remove failed")
	}
}

// ATTENTION: this method can be call after calling of Shutdown method
func (d *DiskManagerImpl) RemoveLogFile() {
	d.logFileMutex.Lock()
	defer d.logFileMutex.Unlock()

	err := os.Remove(d.fileName_log)
	if err != nil {
		fmt.Println(err)
		panic("file remove failed")
	}
}

// erase needless data from log file (use this when db recovery finishes or snapshot finishes)
// file content becomes empty
func (d *DiskManagerImpl) GCLogFile() error {
	d.logFileMutex.Lock()

	d.log.Close()
	d.logFileMutex.Unlock()
	d.RemoveLogFile()
	d.logFileMutex.Lock()
	defer d.logFileMutex.Unlock()

	logfname := d.fileName_log
	file_, err := os.OpenFile(logfname, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalln("can't open log file")
		return err
	}

	_, err = file_.Stat()
	if err != nil {
		log.Fatalln("file info error")
		return err
	}

	d.log = file_

	return nil
}

/**
 * Write the contents of the log into disk file
 * Only return when sync is done, and only perform sequence write
 */
func (d *DiskManagerImpl) WriteLog(log_data []byte) error {
	d.logFileMutex.Lock()
	defer d.logFileMutex.Unlock()

	d.flush_log = true

	// Note: current implementation does not use non-blocking I/O

	d.numFlushes += 1
	_, err := d.log.Write(log_data)

	if err != nil {
		fmt.Println("I/O error while writing log")
		fmt.Println(err)
		// TODO: (SDB) SHOULD BE FIXED: statistics update thread's call causes this error rarely
		return err
	}
	// needs to flush to keep disk file in sync

	d.log.Sync()
	d.flush_log = false

	return nil
}

/**
* Read the contents of the log into the given memory area
* Always read from the beginning and perform sequence read
* @return: false means already reach the end
 */
// Attention: len(log_data) specifies read data length
func (d *DiskManagerImpl) ReadLog(log_data []byte, offset int32, retReadBytes *uint32) bool {
	if int64(offset) >= d.GetLogFileSize() {
		// fmt.Println("end of log file")
		// fmt.Printf("file size is %d\n", d.GetLogFileSize())
		return false
	}

	d.logFileMutex.Lock()
	defer d.logFileMutex.Unlock()

	d.log.Seek(int64(offset), io.SeekStart)
	readBytes, err := d.log.Read(log_data)
	*retReadBytes = uint32(readBytes)

	if err != nil {
		fmt.Println("I/O error at log data reading")
		return false
	}

	return true
}

/**
 * Private helper function to get disk file size
 */
func (d *DiskManagerImpl) GetLogFileSize() int64 {
	d.logFileMutex.Lock()
	defer d.logFileMutex.Unlock()

	fileInfo, err := d.log.Stat()
	if err != nil {
		return -1
	}

	return fileInfo.Size()
}

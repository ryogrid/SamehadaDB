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

	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/types"
)

//DiskManagerImpl is the disk implementation of DiskManager
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

	return &DiskManagerImpl{file, dbFilename, file_1, logfname, nextPageID, 0, fileSize, false, 0}
}

// ShutDown closes of the database file
func (d *DiskManagerImpl) ShutDown() {
	d.db.Close()
	d.log.Close()
}

// Write a page to the database file
func (d *DiskManagerImpl) WritePage(pageId types.PageID, pageData []byte) error {
	offset := int64(pageId * common.PageSize)
	d.db.Seek(offset, io.SeekStart)
	bytesWritten, err := d.db.Write(pageData)
	if err != nil {
		return err
	}

	if bytesWritten != common.PageSize {
		return errors.New("bytes written not equals page size")
	}

	if offset >= d.size {
		d.size = offset + int64(bytesWritten)
	}

	d.db.Sync()
	return nil
}

// Read a page from the database file
func (d *DiskManagerImpl) ReadPage(pageID types.PageID, pageData []byte) error {
	offset := int64(pageID * common.PageSize)

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

	if bytesRead < common.PageSize {
		for i := 0; i < common.PageSize; i++ {
			pageData[i] = 0
		}
	}
	return nil
}

//  AllocatePage allocates a new page
//  For now just keep an increasing counter
func (d *DiskManagerImpl) AllocatePage() types.PageID {
	ret := d.nextPageID
	d.nextPageID++
	return ret
}

// DeallocatePage deallocates page
// Need bitmap in header page for tracking pages
// This does not actually need to do anything for now.
func (d *DiskManagerImpl) DeallocatePage(pageID types.PageID) {
}

// GetNumWrites returns the number of disk writes
func (d *DiskManagerImpl) GetNumWrites() uint64 {
	return d.numWrites
}

// Size returns the size of the file in disk
func (d *DiskManagerImpl) Size() int64 {
	return d.size
}

// ATTENTION: this method can be call after calling of Shutdown method
func (d *DiskManagerImpl) RemoveDBFile() {
	os.Remove(d.fileName)
}

// ATTENTION: this method can be call after calling of Shutdown method
func (d *DiskManagerImpl) RemoveLogFile() {
	os.Remove(d.fileName_log)
}

// TODO: (SDB) need implement WriteLog and ReadLog of DiskManagerImpl for logging/recovery

/**
 * Write the contents of the log into disk file
 * Only return when sync is done, and only perform sequence write
 */
//func (d *DiskManagerImpl) WriteLog(log_data []byte, size int32) {
func (d *DiskManagerImpl) WriteLog(log_data []byte) {
	// enforce swap log buffer

	//assert(log_data != buffer_used)
	//buffer_used := log_data

	// if size == 0 { // no effect on num_flushes_ if log buffer is empty
	// 	return
	// }

	d.flush_log = true

	// Note: current implementation does not use non-blocking I/O
	// if flush_log_f_ != nullptr {
	//   // used for checking non-blocking flushing
	//   assert(flush_log_f_.wait_for(std::chrono::seconds(10)) == std::future_status::ready)
	// }

	d.numFlushes += 1
	// sequence write
	//disk_manager.log.write(log_data, size)
	_, err := d.log.Write(log_data)

	// check for I/O error
	if err != nil {
		fmt.Println("I/O error while writing log")
		return
	}
	// needs to flush to keep disk file in sync
	//disk_manager.log.Flush()
	d.log.Sync()
	d.flush_log = false
}

/**
* Read the contents of the log into the given memory area
* Always read from the beginning and perform sequence read
* @return: false means already reach the end
 */
// Attention: len(log_data) specifies read data length
func (d *DiskManagerImpl) ReadLog(log_data []byte, offset int32) bool {
	if int64(offset) >= d.GetLogFileSize() {
		fmt.Println("end of log file")
		fmt.Printf("file size is %d\n", d.GetLogFileSize())
		return false
	}

	d.log.Seek(int64(offset), io.SeekStart)
	readBytes, err := d.log.Read(log_data)
	fmt.Printf("readBytes %d\n", readBytes)
	// if log file ends before reading "size"
	//read_count := d.log.gcount()
	if readBytes < len(log_data) {
		d.log.Close()
		//memset(log_data+read_count, 0, size-read_count)
		log_data[readBytes] = byte(len(log_data) - readBytes)
	}

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
	/*
		struct stat stat_buf;
		int rc = stat(file_name.c_str(), &stat_buf);
		return rc == 0 ? static_cast<int>(stat_buf.st_size) : -1;
	*/
	fileInfo, err := d.log.Stat()
	if err != nil {
		return -1
	}

	return fileInfo.Size()
}

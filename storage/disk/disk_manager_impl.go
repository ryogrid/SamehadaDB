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

	return &DiskManagerImpl{file, dbFilename, file_1, logfname, nextPageID, 0, fileSize}
}

// ShutDown closes of the database file
func (d *DiskManagerImpl) ShutDown() {
	fmt.Println(d.db)
	fmt.Println(d.log)
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
// /**
//  * Write the contents of the log into disk file
//  * Only return when sync is done, and only perform sequence write
//  */
//  void DiskManager::WriteLog(char *log_data, int size) {
// 	// enforce swap log buffer
// 	assert(log_data != buffer_used);
// 	buffer_used = log_data;

// 	if (size == 0) {  // no effect on num_flushes_ if log buffer is empty
// 	  return;
// 	}

// 	flush_log_ = true;

// 	if (flush_log_f_ != nullptr) {
// 	  // used for checking non-blocking flushing
// 	  assert(flush_log_f_->wait_for(std::chrono::seconds(10)) == std::future_status::ready);
// 	}

// 	num_flushes_ += 1;
// 	// sequence write
// 	log_io_.write(log_data, size);

// 	// check for I/O error
// 	if (log_io_.bad()) {
// 	  LOG_DEBUG("I/O error while writing log");
// 	  return;
// 	}
// 	// needs to flush to keep disk file in sync
// 	log_io_.flush();
// 	flush_log_ = false;
//   }

//   /**
//    * Read the contents of the log into the given memory area
//    * Always read from the beginning and perform sequence read
//    * @return: false means already reach the end
//    */
//   bool DiskManager::ReadLog(char *log_data, int size, int offset) {
// 	if (offset >= GetFileSize(log_name_)) {
// 	  // LOG_DEBUG("end of log file");
// 	  // LOG_DEBUG("file size is %d", GetFileSize(log_name_));
// 	  return false;
// 	}
// 	log_io_.seekp(offset);
// 	log_io_.read(log_data, size);
// 	// if log file ends before reading "size"
// 	int read_count = log_io_.gcount();
// 	if (read_count < size) {
// 	  log_io_.clear();
// 	  memset(log_data + read_count, 0, size - read_count);
// 	}

// 	return true;
//   }

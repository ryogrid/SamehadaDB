package recovery

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/disk"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * LogManager maintains a separate thread that is awakened whenever the log buffer is full or whenever a timeout
 * happens. When the thread is awakened, the log buffer's content is written into the disk log file.
 */
type LogManager struct {
	// TODO(students): you may add your own member variables

	// TODO: (SDB) must ensure atomicity
	offset uint32
	// TODO: (SDB) must ensure atomicity
	log_buffer_lsn types.LSN
	/** The atomic counter which records the next log sequence number. */
	// TODO: (SDB) must ensure atomicity
	next_lsn types.LSN
	/** The log records before and including the persistent lsn have been written to disk. */
	// TODO: (SDB) must ensure atomicity
	persistent_lsn types.LSN
	log_buffer     []byte
	flush_buffer   []byte
	latch          common.ReaderWriterLatch
	//flush_thread   *thread //__attribute__((__unused__));
	//cv           condition_variable
	disk_manager *disk.DiskManager //__attribute__((__unused__));
}

func NewLogManager(disk_manager *disk.DiskManager) *LogManager {
	ret := new(LogManager)
	ret.next_lsn = 0
	ret.persistent_lsn = common.InvalidLSN
	ret.disk_manager = disk_manager
	ret.log_buffer = make([]byte, common.LogBufferSize)
	ret.flush_buffer = make([]byte, common.LogBufferSize)
	ret.offset = 0
	return ret
}

func (log_manager *LogManager) GetNextLSN() types.LSN          { return log_manager.next_lsn }
func (log_manager *LogManager) GetPersistentLSN() types.LSN    { return log_manager.persistent_lsn }
func (log_manager *LogManager) SetPersistentLSN(lsn types.LSN) { log_manager.persistent_lsn = lsn }
func (log_manager *LogManager) GetLogBuffer() []byte           { return log_manager.log_buffer }

func (log_manager *LogManager) Flush() {
	//TODO: (SDB) not ported yet
	/*
		//unique_lock lock(log_manager.latch)
		log_manager.latch.WLock()
		lsn = log_manager.log_buffer_lsn.load()
		offset = log_manager.offset.load()
		offset = 0
		//access.unlock()
		log_manager.latch.WUnlock()
		swap(log_manager.log_buffer, log_manager.flush_buffer)
		printf("offset:%lu\n", offset)
		disk_manager.WriteLog(log_manager.flush_buffer, offset)
		log_manager.persistent_lsn = lsn
	*/
}

/*
* set enable_logging = true
* Start a separate thread to execute flush to disk operation periodically
* The flush can be triggered when the log buffer is full or buffer pool
* manager wants to force flush (it only happens when the flushed page has a
* larger LSN than persistent LSN)
 */
func (log_manager *LogManager) RunFlushThread() { common.EnableLogging = true }

/*
* Stop and join the flush thread, set enable_logging = false
 */
func (log_manager *LogManager) StopFlushThread() { common.EnableLogging = false }

/*
* append a log record into log buffer
* you MUST set the log record's lsn within this method
* @return: lsn that is assigned to this log record
*
*
* example below
* // First, serialize the must have fields(20 bytes in total)
* log_record.lsn_ = next_lsn_++;
* memcpy(log_buffer_ + offset_, &log_record, 20);
* int pos = offset_ + 20;
*
* if (log_record.log_record_type_ == LogRecordType::INSERT) {
*    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
*    pos += sizeof(RID);
*    // we have provided serialize function for tuple class
*    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
*  }
*
 */
func (log_manager *LogManager) AppendLogRecord(log_record *LogRecord) types.LSN {
	// First, serialize the must have fields(20 bytes in total)
	// std::unique_lock lock(latch_);

	//TODO: (SDB) not ported yet
	/*
		if (LOG_BUFFER_SIZE - log_manager.offset < HEADER_SIZE) {
			log_manager.Flush()
		}
		log_record.lsn = next_lsn++
		memcpy(log_manager.log_buffer + offset, log_record, HEADER_SIZE)
		if ((int32_t)(LOG_BUFFER_SIZE - offset) < log_record.size) {
			log_manager.Flush()
			// do it again in new buffer
			memcpy(log_manager.log_buffer + offset, log_record, HEADER_SIZE)
		}
		log_manager.log_buffer_lsn = log_record.lsn
		pos  := offset + LogRecord::HEADER_SIZE
		offset += log_record.size
		// access.unlock();

		if (log_record.log_record_type_ == LogRecordType::INSERT) {
			memcpy(log_manager.log_buffer + pos, &log_record.insert_rid, sizeof(RID))
			pos += sizeof(RID)
			// we have provided serialize function for tuple class
			log_record.insert_tuple.SerializeTo(log_manager.log_buffer + pos)
		} else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE ||
					log_record.log_record_type_ == LogRecordType::MARKDELETE ||
					log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
			memcpy(log_manager.log_buffer + pos, &log_record.delete_rid, sizeof(RID))
			pos += sizeof(RID);
			// we have provided serialize function for tuple class
			log_record.delete_tuple.SerializeTo(log_manager.log_buffer_ + pos)
		} else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
			memcpy(log_buffer + pos, &log_record.update_rid_, sizeof(RID))
			pos += sizeof(RID)
			// we have provided serialize function for tuple class
			log_record.old_tuple.SerializeTo(log_manager.log_buffer + pos)
			pos += sizeof(log_record.old_tuple.GetLength() + sizeof(uint32)))
			log_record.new_tuple.SerializeTo(log_buffer + pos)
		} else if (log_record.log_record_type == LogRecordType::NEWPAGE) {
			memcpy(log_manager.log_buffer + pos, &log_record.prev_page_id, sizeof(PageID))
		}
	*/
	return log_record.lsn
}

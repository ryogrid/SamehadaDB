package recovery

/**
 * LogManager maintains a separate thread that is awakened whenever the log buffer is full or whenever a timeout
 * happens. When the thread is awakened, the log buffer's content is written into the disk log file.
 */
type LogManager struct {
	 // TODO(students): you may add your own member variables
	offset atomic<uint32>
	log_buffer_lsn atomic<LSN>
	/** The atomic counter which records the next log sequence number. */
	next_lsn atomic<LSN>
	/** The log records before and including the persistent lsn have been written to disk. */
	persistent_lsn atomic<LSN>
	log_buffer *char
	flush_buffer *char
	latch mutex
	flush_thread *thread //__attribute__((__unused__));
	cv condition_variable
	disk_manager *DiskManager //__attribute__((__unused__));
};

func New(disk_manager *DiskManager) *LogManager {
	next_lsn_(0)
	persistent_lsn_(INVALID_LSN)
	disk_manager_(disk_manager)
	log_buffer_ = new char[LOG_BUFFER_SIZE]
	flush_buffer = new char[LOG_BUFFER_SIZE]
	offset_ = 0
}

func (log_manager *LogManager) GetNextLSN() LSN { return log_manager.next_lsn }
func (log_manager *LogManager) GetPersistentLSN() LSN { return log_manager.persistent_lsn }
func (log_manager *LogManager) SetPersistentLSN(lsn LSN) { log_manager.persistent_lsn = lsn }
func (log_manager *LogManager) GetLogBuffer() *char { return log_manager.log_buffer }

func (log_manager *LogManager) Flush() {
	unique_lock lock(latch_)
	lsn = log_manager.log_buffer_lsn.load()
	offset = log_manager.offset.load()
	offset = 0
	lock.unlock()
	swap(log_manager.log_buffer, log_manager.flush_buffer)
	printf("offset:%lu\n", offset);
	disk_manager.WriteLog(log_manager.flush_buffer, offset);
	log_manager.persistent_lsn = lsn;
}

/*
* set enable_logging = true
* Start a separate thread to execute flush to disk operation periodically
* The flush can be triggered when the log buffer is full or buffer pool
* manager wants to force flush (it only happens when the flushed page has a
* larger LSN than persistent LSN)
*/
func (log_manager *LogManager) RunFlushThread() { enable_logging = true }

/*
* Stop and join the flush thread, set enable_logging = false
*/
func (log_manager *LogManager) StopFlushThread() { enable_logging = false }

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
func (log_manager *LogManager) AppendLogRecord(log_record *LogRecord) LSN {
	// First, serialize the must have fields(20 bytes in total)
	// std::unique_lock lock(latch_);
	if (LOG_BUFFER_SIZE - offset < LogRecord::HEADER_SIZE) {
		Flush()
	}
	log_record.lsn_ = next_lsn++
	memcpy(log_manager.log_buffer + offset, log_record, LogRecord::HEADER_SIZE)
	if ((int32_t)(LOG_BUFFER_SIZE - offset) < log_record.size) {
		log_manager.Flush()
		// do it again in new buffer
		memcpy(log_manager.log_buffer + offset, log_record, LogRecord::HEADER_SIZE)
	}
	log_manager.log_buffer_lsn = log_record.lsn
	pos  := offset + LogRecord::HEADER_SIZE
	offset += log_record.size
	// lock.unlock();

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

	return log_record.lsn
}

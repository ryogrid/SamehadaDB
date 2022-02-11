package recovery

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/disk"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * Read log file from disk, redo and undo.
 */
type LogRecovery struct {
	disk_manager        *disk.DiskManager         //__attribute__((__unused__))
	buffer_pool_manager *buffer.BufferPoolManager //__attribute__((__unused__))

	/** Maintain active transactions and its corresponding latest lsn. */
	active_txn map[types.TxnID]types.LSN
	/** Mapping the log sequence number to log file offset for undos. */
	lsn_mapping map[types.LSN]int

	offset     int32 //__attribute__((__unused__))
	log_buffer []byte
}

/*
public:
LogRecovery(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager)
	: disk_manager(disk_manager), buffer_pool_manager(buffer_pool_manager), offset(0) {
  log_buffer = new char[LOG_BUFFER_SIZE]
}

~LogRecovery() {
  delete[] log_buffer
  log_buffer = nullptr
}

func Redo()
func Undo()
bool DeserializeLogRecord(const char *data, LogRecord *log_record)
*/

func NewLogRecovery(disk_manager *disk.DiskManager, buffer_pool_manager *buffer.BufferPoolManager) *LogRecovery {
	return &LogRecovery{disk_manager, buffer_pool_manager, make(map[types.TxnID]types.LSN), make(map[types.LSN]int), 0, make([]byte, common.LogBufferSize)}
}

/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
func (log_recovery *LogRecovery) DeserializeLogRecord(data []byte, log_record *LogRecord) bool {
	// TODO: (SDB) [logging/recovery] not ported yet
	/*
		if (LOG_BUFFER_SIZE - (data - log_buffer) < LogRecord::HEADER_SIZE) {
		  return false
		}
		// First, unserialize the must have fields(20 bytes in total)
		memcpy(log_record, data, LogRecord::HEADER_SIZE)
		if (log_record.size_ <= 0) {
		  return false
		}
		if (LOG_BUFFER_SIZE - (data - log_buffer) < log_record.size) {
		  return false
		}
		int pos = LogRecord::HEADER_SIZE
		if (log_record.log_record_type_ == LogRecordType::INSERT) {
		  memcpy(&log_record.insert_rid, data + pos, sizeof(RID))
		  pos += sizeof(RID)
		  // we have provided serialize function for tuple class
		  log_record.insert_tuple.DeserializeFrom(data + pos)
		} else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE ||
				   log_record.log_record_type_ == LogRecordType::MARKDELETE ||
				   log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
		  memcpy(&log_record.delete_rid, data + pos, sizeof(RID))
		  pos += sizeof(RID)
		  // we have provided serialize function for tuple class
		  log_record.delete_tuple.DeserializeFrom(data + pos)
		} else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
		  memcpy(&log_record.update_rid, data + pos, sizeof(RID))
		  pos += sizeof(RID)
		  // we have provided serialize function for tuple class
		  log_record.old_tuple.DeserializeFrom(data + pos)
		  pos += sizeof(log_record.old_tuple.GetLength() + sizeof(uint32_t))
		  log_record.new_tuple.DeserializeFrom(data + pos)
		} else if (log_record.log_record_type_ == LogRecordType::NEWPAGE) {
		  memcpy(&log_record.prev_page_id, data + pos, sizeof(page_id_t))
		}

		return true
	*/
	return false
}

/*
*redo phase on TABLE PAGE level(table/table_page.h)
*read log file from the beginning to end (you must prefetch log records into
*log buffer to reduce unnecessary I/O operations), remember to compare page's
*LSN with log_record's sequence number, and also build active_txn table &
*lsn_mapping table
 */
func (log_recovery *LogRecovery) Redo() {
	// TODO: (SDB) [logging/recovery] not ported yet
	/*
		int file_offset = 0
		while (disk_manager.ReadLog(log_buffer, LOG_BUFFER_SIZE, file_offset)) {
		  int buffer_offset = 0
		  LogRecord log_record
		  while (DeserializeLogRecord(log_buffer + buffer_offset, &log_record)) {
			active_txn[log_record.txn_id] = log_record.lsn
			lsn_mapping[log_record.lsn] = file_offset + buffer_offset
			if (log_record.log_record_type_ == LogRecordType::INSERT) {
			  auto page =
				  static_cast<TablePage *>(buffer_pool_manager.FetchPage(log_record.insert_rid.GetPageId(), nullptr))
			  if (page.GetLSN() < log_record.GetLSN()) {
				page.InsertTuple(log_record.insert_tuple, &log_record.insert_rid, nullptr, nullptr, nullptr)
				page.SetLSN(log_record.GetLSN())
			  }
			  buffer_pool_manager.UnpinPage(log_record.insert_rid.GetPageId(), true, nullptr)
			} else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE) {
			  auto page =
				  static_cast<TablePage *>(buffer_pool_manager.FetchPage(log_record.delete_rid.GetPageId(), nullptr))
			  if (page.GetLSN() < log_record.GetLSN()) {
				page.ApplyDelete(log_record.delete_rid, nullptr, nullptr)
				page.SetLSN(log_record.GetLSN())
			  }
			  buffer_pool_manager.UnpinPage(log_record.delete_rid.GetPageId(), true, nullptr)
			} else if (log_record.log_record_type_ == LogRecordType::MARKDELETE) {
			  auto page =
				  static_cast<TablePage *>(buffer_pool_manager.FetchPage(log_record.delete_rid.GetPageId(), nullptr))
			  if (page.GetLSN() < log_record.GetLSN()) {
				page.MarkDelete(log_record.delete_rid, nullptr, nullptr, nullptr)
				page.SetLSN(log_record.GetLSN())
			  }
			  buffer_pool_manager.UnpinPage(log_record.delete_rid.GetPageId(), true, nullptr)
			} else if (log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
			  auto page =
				  static_cast<TablePage *>(buffer_pool_manager.FetchPage(log_record.delete_rid.GetPageId(), nullptr))
			  if (page.GetLSN() < log_record.GetLSN()) {
				page.RollbackDelete(log_record.delete_rid, nullptr, nullptr)
				page.SetLSN(log_record.GetLSN())
			  }
			  buffer_pool_manager.UnpinPage(log_record.delete_rid.GetPageId(), true, nullptr)
			} else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
			  auto page =
				  static_cast<TablePage *>(buffer_pool_manager.FetchPage(log_record.update_rid.GetPageId(), nullptr))
			  if (page.GetLSN() < log_record.GetLSN()) {
				page.UpdateTuple(log_record.new_tuple, &log_record.old_tuple, log_record.update_rid, nullptr, nullptr,
								  nullptr)
				page.SetLSN(log_record.GetLSN())
			  }
			  buffer_pool_manager.UnpinPage(log_record.update_rid.GetPageId(), true, nullptr)
			} else if (log_record.log_record_type_ == LogRecordType::BEGIN) {
			  active_txn[log_record.txn_id] = log_record.lsn
			} else if (log_record.log_record_type_ == LogRecordType::COMMIT) {
			  active_txn.erase(log_record.txn_id)
			} else if (log_record.log_record_type_ == LogRecordType::NEWPAGE) {
			  page_id_t page_id
			  auto new_page = static_cast<TablePage *>(buffer_pool_manager.NewPage(&page_id, nullptr))
			  LOG_DEBUG("page_id: %d", page_id)
			  new_page.Init(page_id, PAGE_SIZE, log_record.prev_page_id, nullptr, nullptr)
			  buffer_pool_manager.UnpinPage(page_id, true, nullptr)
			}
			buffer_offset += log_record.size
		  }
		  // incomplete log record
		  file_offset += buffer_offset
		}
	*/
}

/*
*undo phase on TABLE PAGE level(table/table_page.h)
*iterate through active txn map and undo each operation
 */
func (log_recovery *LogRecovery) Undo() {
	// TODO: (SDB) [logging/recovery] not ported yet
	/*
		LogRecord log_record
		for (auto it : active_txn) {
			auto lsn = it.second
			while (lsn != INVALID_LSN) {
			auto file_offset = lsn_mapping[lsn]
			LOG_DEBUG("file_offset: %d", file_offset)
			disk_manager.ReadLog(log_buffer, LOG_BUFFER_SIZE, file_offset)
			DeserializeLogRecord(log_buffer, &log_record)
			if (log_record.log_record_type_ == LogRecordType::INSERT) {
				auto page =
					static_cast<TablePage *>(buffer_pool_manager.FetchPage(log_record.insert_rid.GetPageId(), nullptr))
				LOG_DEBUG("insert log type, page lsn:%d, log lsn:%d", page.GetLSN(), log_record.GetLSN())
				page.ApplyDelete(log_record.insert_rid, nullptr, nullptr)
				buffer_pool_manager.UnpinPage(log_record.insert_rid.GetPageId(), true, nullptr)
			} else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE) {
				auto page =
					static_cast<TablePage *>(buffer_pool_manager.FetchPage(log_record.delete_rid.GetPageId(), nullptr))
				page.InsertTuple(log_record.delete_tuple, &log_record.delete_rid, nullptr, nullptr, nullptr)
				buffer_pool_manager.UnpinPage(log_record.delete_rid.GetPageId(), true, nullptr)
			} else if (log_record.log_record_type_ == LogRecordType::MARKDELETE) {
				auto page =
					static_cast<TablePage *>(buffer_pool_manager.FetchPage(log_record.delete_rid.GetPageId(), nullptr))
				page.RollbackDelete(log_record.delete_rid, nullptr, nullptr)
				buffer_pool_manager.UnpinPage(log_record.delete_rid.GetPageId(), true, nullptr)
			} else if (log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
				auto page =
					static_cast<TablePage *>(buffer_pool_manager.FetchPage(log_record.delete_rid.GetPageId(), nullptr))
				page.MarkDelete(log_record.delete_rid, nullptr, nullptr, nullptr)
				buffer_pool_manager.UnpinPage(log_record.delete_rid.GetPageId(), true, nullptr)
			} else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
				auto page =
					static_cast<TablePage *>(buffer_pool_manager.FetchPage(log_record.update_rid.GetPageId(), nullptr))
				page.UpdateTuple(log_record.old_tuple, &log_record.new_tuple, log_record.update_rid, nullptr, nullptr,
								nullptr)
				buffer_pool_manager.UnpinPage(log_record.update_rid.GetPageId(), true, nullptr)
			}
			lsn = log_record.prev_lsn
			}
		}
		buffer_pool_manager.FlushAllPages()
	*/
}

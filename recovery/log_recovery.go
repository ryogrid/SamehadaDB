package recovery

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/access"
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

	if common.LogBufferSize-len(data) < int(HEADER_SIZE) {
		return false
	}
	// First, unserialize the must have fields(20 bytes in total)
	log_record = new(LogRecord)
	record_construct_buf := new(bytes.Buffer)
	//memcpy(log_record, data, HEADER_SIZE)
	//copy(record_construct_buf, data[:HEADER_SIZE])
	record_construct_buf.Write(data[:HEADER_SIZE])
	//binary.Write(record_construct_buf, binary.LittleEndian,)
	binary.Read(record_construct_buf, binary.LittleEndian, &log_record)

	if log_record.size <= 0 {
		return false
	}
	if common.LogBufferSize-len(data) < int(log_record.size) {
		return false
	}

	//binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &ret)
	pos := HEADER_SIZE
	if log_record.log_record_type == INSERT {
		//memcpy(&log_record.insert_rid, data+pos, sizeof(RID))
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, log_record.insert_rid)
		pos += uint32(unsafe.Sizeof(log_record.insert_rid))
		// we have provided serialize function for tuple class
		log_record.insert_tuple.DeserializeFrom(data[pos:])
	} else if log_record.log_record_type == APPLYDELETE ||
		log_record.log_record_type == MARKDELETE ||
		log_record.log_record_type == ROLLBACKDELETE {
		//memcpy(&log_record.delete_rid, data+pos, sizeof(RID))
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, log_record.delete_rid)
		pos += uint32(unsafe.Sizeof(log_record.delete_rid))
		// we have provided serialize function for tuple class
		log_record.delete_tuple.DeserializeFrom(data[pos:])
	} else if log_record.log_record_type == UPDATE {
		//memcpy(&log_record.update_rid, data+pos, sizeof(RID))
		//pos += sizeof(RID)
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, log_record.update_rid)
		pos += uint32(unsafe.Sizeof(log_record.update_rid))
		// we have provided serialize function for tuple class
		log_record.old_tuple.DeserializeFrom(data[pos:])
		pos += log_record.old_tuple.Size() + uint32(unsafe.Sizeof(log_record.update_rid))
		log_record.new_tuple.DeserializeFrom(data[pos:])
	} else if log_record.log_record_type == NEWPAGE {
		//memcpy(&log_record.prev_page_id, data+pos, sizeof(page_id_t))
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, log_record.prev_page_id)
	}

	return true
}

// TODO: (SDB) not ported route exists at Redo method of LogRecovery class.
//             when log_record_type is APPLYDELETE, MARKDELETE, ROLLBACKDELETE, UPDATE

/*
*redo phase on TABLE PAGE level(table/table_page.h)
*read log file from the beginning to end (you must prefetch log records into
*log buffer to reduce unnecessary I/O operations), remember to compare page's
*LSN with log_record's sequence number, and also build active_txn table &
*lsn_mapping table
 */
func (log_recovery *LogRecovery) Redo() {
	// TODO: (SDB) [logging/recovery] not ported yet

	log_recovery.log_buffer = make([]byte, common.LogBufferSize)
	var file_offset uint32 = 0
	for (*log_recovery.disk_manager).ReadLog(log_recovery.log_buffer, int32(file_offset)) {
		var buffer_offset uint32 = 0
		var log_record LogRecord
		for log_recovery.DeserializeLogRecord(log_recovery.log_buffer[buffer_offset:], &log_record) {
			log_recovery.active_txn[log_record.txn_id] = log_record.lsn
			log_recovery.lsn_mapping[log_record.lsn] = int(file_offset + buffer_offset)
			if log_record.log_record_type == INSERT {
				page :=
					access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.insert_rid.GetPageId()))
				if page.GetLSN() < log_record.GetLSN() {
					page.InsertTuple(&log_record.insert_tuple, nil, nil, nil)
					page.SetLSN(log_record.GetLSN())
				}
				log_recovery.buffer_pool_manager.UnpinPage(log_record.insert_rid.GetPageId(), true)
				// } else if log_record.log_record_type == APPLYDELETE {
				// 	page :=
				// 		access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.delete_rid.GetPageId()))
				// 	if page.GetLSN() < log_record.GetLSN() {
				// 		page.ApplyDelete(log_record.delete_rid, nil, nil)
				// 		page.SetLSN(log_record.GetLSN())
				// 	}
				// 	log_recovery.buffer_pool_manager.UnpinPage(log_record.delete_rid.GetPageId(), true)
				// } else if log_record.log_record_type == MARKDELETE {
				// 	page :=
				// 		access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.delete_rid.GetPageId()))
				// 	if page.GetLSN() < log_record.GetLSN() {
				// 		page.MarkDelete(log_record.delete_rid, nil, nil, nil)
				// 		page.SetLSN(log_record.GetLSN())
				// 	}
				// 	log_recovery.buffer_pool_manager.UnpinPage(log_record.delete_rid.GetPageId(), true)
				// } else if log_record.log_record_type == ROLLBACKDELETE {
				// 	page :=
				// 		access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.delete_rid.GetPageId()))
				// 	if page.GetLSN() < log_record.GetLSN() {
				// 		page.RollbackDelete(log_record.delete_rid, nil, nil)
				// 		page.SetLSN(log_record.GetLSN())
				// 	}
				// 	log_recovery.buffer_pool_manager.UnpinPage(log_record.delete_rid.GetPageId(), true)
				// } else if log_record.log_record_type == UPDATE {
				// 	page :=
				// 		access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.update_rid.GetPageId()))
				// 	if page.GetLSN() < log_record.GetLSN() {
				// 		page.UpdateTuple(log_record.new_tuple, &log_record.old_tuple, log_record.update_rid, nil, nil, nil)
				// 		page.SetLSN(log_record.GetLSN())
				// 	}
				// 	log_recovery.buffer_pool_manager.UnpinPage(log_record.update_rid.GetPageId(), true)
			} else if log_record.log_record_type == BEGIN {
				log_recovery.active_txn[log_record.txn_id] = log_record.lsn
			} else if log_record.log_record_type == COMMIT {
				delete(log_recovery.active_txn, log_record.txn_id)
			} else if log_record.log_record_type == NEWPAGE {
				var page_id types.PageID
				//new_page := access.CastPageAsTablePage(log_recovery.buffer_pool_manager.NewPage(&page_id, nil))
				new_page := access.CastPageAsTablePage(log_recovery.buffer_pool_manager.NewPage())
				page_id = new_page.GetPageId()
				fmt.Printf("page_id: %d\n", page_id)
				new_page.Init(page_id, log_record.prev_page_id, nil, nil, nil)
				log_recovery.buffer_pool_manager.UnpinPage(page_id, true)
			}
			buffer_offset += log_record.size
		}
		// incomplete log record
		file_offset += buffer_offset
	}

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
			if (log_record.log_record_type == LogRecordType::INSERT) {
				auto page =
					static_cast<TablePage *>(log_recovery.buffer_pool_manager.FetchPage(log_record.insert_rid.GetPageId(), nil))
				LOG_DEBUG("insert log type, page lsn:%d, log lsn:%d", page.GetLSN(), log_record.GetLSN())
				page.ApplyDelete(log_record.insert_rid, nil, nil)
				log_recovery.buffer_pool_manager.UnpinPage(log_record.insert_rid.GetPageId(), true, nil)
			} else if (log_record.log_record_type == LogRecordType::APPLYDELETE) {
				auto page =
					static_cast<TablePage *>(log_recovery.buffer_pool_manager.FetchPage(log_record.delete_rid.GetPageId(), nil))
				page.InsertTuple(log_record.delete_tuple, &log_record.delete_rid, nil, nil, nil)
				log_recovery.buffer_pool_manager.UnpinPage(log_record.delete_rid.GetPageId(), true, nil)
			} else if (log_record.log_record_type == LogRecordType::MARKDELETE) {
				auto page =
					static_cast<TablePage *>(log_recovery.buffer_pool_manager.FetchPage(log_record.delete_rid.GetPageId(), nil))
				page.RollbackDelete(log_record.delete_rid, nil, nil)
				log_recovery.buffer_pool_manager.UnpinPage(log_record.delete_rid.GetPageId(), true, nil)
			} else if (log_record.log_record_type == LogRecordType::ROLLBACKDELETE) {
				auto page =
					static_cast<TablePage *>(log_recovery.buffer_pool_manager.FetchPage(log_record.delete_rid.GetPageId(), nil))
				page.MarkDelete(log_record.delete_rid, nil, nil, nil)
				log_recovery.buffer_pool_manager.UnpinPage(log_record.delete_rid.GetPageId(), true, nil)
			} else if (log_record.log_record_type == LogRecordType::UPDATE) {
				auto page =
					static_cast<TablePage *>(log_recovery.buffer_pool_manager.FetchPage(log_record.update_rid.GetPageId(), nil))
				page.UpdateTuple(log_record.old_tuple, &log_record.new_tuple, log_record.update_rid, nil, nil,
								nil)
				log_recovery.buffer_pool_manager.UnpinPage(log_record.update_rid.GetPageId(), true, nil)
			}
			lsn = log_record.prev_lsn
			}
		}
		log_recovery.buffer_pool_manager.FlushAllPages()
	*/
}

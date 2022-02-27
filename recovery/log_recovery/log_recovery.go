package log_recovery

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/recovery"
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
func (log_recovery *LogRecovery) DeserializeLogRecord(data []byte, log_record *recovery.LogRecord) bool {
	// if common.LogBufferSize-len(data) < int(recovery.HEADER_SIZE) {
	// 	fmt.Printf("len(data) = %d\n", len(data))
	// 	fmt.Println("return false point 1")
	// 	return false
	// }
	// First, unserialize the must have fields(20 bytes in total)
	//log_record = new(recovery.LogRecord)
	record_construct_buf := new(bytes.Buffer)
	//memcpy(log_record, data, HEADER_SIZE)
	//copy(record_construct_buf, data[:HEADER_SIZE])
	record_construct_buf.Write(data[:recovery.HEADER_SIZE])
	//binary.Write(record_construct_buf, binary.LittleEndian,)
	//binary.Read(record_construct_buf, binary.LittleEndian, &log_record)
	binary.Read(record_construct_buf, binary.LittleEndian, &(log_record.Size))
	binary.Read(record_construct_buf, binary.LittleEndian, &(log_record.Lsn))
	binary.Read(record_construct_buf, binary.LittleEndian, &(log_record.Txn_id))
	binary.Read(record_construct_buf, binary.LittleEndian, &(log_record.Prev_lsn))
	binary.Read(record_construct_buf, binary.LittleEndian, &(log_record.Log_record_type))

	if log_record.Size <= 0 {
		fmt.Println(log_record)
		fmt.Println("return false point 2")
		return false
	}
	// if common.LogBufferSize-len(data) < int(log_record.Size) {
	// 	fmt.Println("return false point 3")
	// 	return false
	// }

	//binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &ret)
	pos := recovery.HEADER_SIZE
	if log_record.Log_record_type == recovery.INSERT {
		//memcpy(&log_record.Insert_rid, data+pos, sizeof(RID))
		//buf := bytes.NewBuffer(data[pos : pos+uint32(unsafe.Sizeof(log_record.Insert_rid))])
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, &log_record.Insert_rid)
		pos += uint32(unsafe.Sizeof(log_record.Insert_rid))
		// we have provided serialize function for tuple class
		log_record.Insert_tuple.DeserializeFrom(data[pos:])
	} else if log_record.Log_record_type == recovery.APPLYDELETE ||
		log_record.Log_record_type == recovery.MARKDELETE ||
		log_record.Log_record_type == recovery.ROLLBACKDELETE {
		//memcpy(&log_record.Delete_rid, data+pos, sizeof(RID))
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, &log_record.Delete_rid)
		pos += uint32(unsafe.Sizeof(log_record.Delete_rid))
		// we have provided serialize function for tuple class
		log_record.Delete_tuple.DeserializeFrom(data[pos:])
	} else if log_record.Log_record_type == recovery.UPDATE {
		//memcpy(&log_record.Update_rid, data+pos, sizeof(RID))
		//pos += sizeof(RID)
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, &log_record.Update_rid)
		pos += uint32(unsafe.Sizeof(log_record.Update_rid))
		// we have provided serialize function for tuple class
		log_record.Old_tuple.DeserializeFrom(data[pos:])
		pos += log_record.Old_tuple.Size() + uint32(unsafe.Sizeof(log_record.Update_rid))
		log_record.New_tuple.DeserializeFrom(data[pos:])
	} else if log_record.Log_record_type == recovery.NEWPAGE {
		//memcpy(&log_record.Prev_page_id, data+pos, sizeof(page_id_t))
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, &log_record.Prev_page_id)
	}

	//fmt.Println(log_record)

	return true
}

// TODO: (SDB) not ported route exists at Redo method of LogRecovery class.
//             when Log_record_type is APPLYDELETE, MARKDELETE, ROLLBACKDELETE, UPDATE

/*
*redo phase on TABLE PAGE level(table/table_page.h)
*read log file from the beginning to end (you must prefetch log records into
*log buffer to reduce unnecessary I/O operations), remember to compare page's
*LSN with log_record's sequence number, and also build active_txn table &
*lsn_mapping table
 */
func (log_recovery *LogRecovery) Redo() {
	readLogLoopCnt := 0
	deserializeLoopCnt := 0
	log_recovery.log_buffer = make([]byte, common.LogBufferSize)
	var file_offset uint32 = 0
	for (*log_recovery.disk_manager).ReadLog(log_recovery.log_buffer, int32(file_offset)) {
		var buffer_offset uint32 = 0
		var log_record recovery.LogRecord
		fmt.Printf("outer file_offset %d\n", file_offset)
		readLogLoopCnt++
		if readLogLoopCnt > 100 {
			//fmt.Printf("file_offset %d\n", file_offset)
			panic("readLogLoopCnt is illegal")
		}
		for log_recovery.DeserializeLogRecord(log_recovery.log_buffer[buffer_offset:], &log_record) {
			deserializeLoopCnt++
			fmt.Printf("inner file_offset %d\n", file_offset)
			fmt.Printf("inner buffer_offset %d\n", buffer_offset)
			fmt.Println(log_record)
			// if deserializeLoopCnt > 100 {
			// 	fmt.Printf("file_offset %d\n", file_offset)
			// 	fmt.Printf("buffer_offset %d\n", buffer_offset)
			// 	fmt.Println(log_record)
			// 	panic("deserializeLoopCnt is illegal")
			// }
			log_recovery.active_txn[log_record.Txn_id] = log_record.Lsn
			log_recovery.lsn_mapping[log_record.Lsn] = int(file_offset + buffer_offset)
			if log_record.Log_record_type == recovery.INSERT {
				page :=
					access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.Insert_rid.GetPageId()))
				if page.GetLSN() < log_record.GetLSN() {
					log_record.Insert_tuple.SetRID(&log_record.Insert_rid)
					page.InsertTuple(&log_record.Insert_tuple, nil, nil, nil)
					page.SetLSN(log_record.GetLSN())
				}
				log_recovery.buffer_pool_manager.UnpinPage(log_record.Insert_rid.GetPageId(), true)
				// } else if log_record.Log_record_type == APPLYDELETE {
				// 	page :=
				// 		access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.Delete_rid.GetPageId()))
				// 	if page.GetLSN() < log_record.GetLSN() {
				// 		page.ApplyDelete(log_record.Delete_rid, nil, nil)
				// 		page.SetLSN(log_record.GetLSN())
				// 	}
				// 	log_recovery.buffer_pool_manager.UnpinPage(log_record.Delete_rid.GetPageId(), true)
				// } else if log_record.Log_record_type == MARKDELETE {
				// 	page :=
				// 		access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.Delete_rid.GetPageId()))
				// 	if page.GetLSN() < log_record.GetLSN() {
				// 		page.MarkDelete(log_record.Delete_rid, nil, nil, nil)
				// 		page.SetLSN(log_record.GetLSN())
				// 	}
				// 	log_recovery.buffer_pool_manager.UnpinPage(log_record.Delete_rid.GetPageId(), true)
				// } else if log_record.Log_record_type == ROLLBACKDELETE {
				// 	page :=
				// 		access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.Delete_rid.GetPageId()))
				// 	if page.GetLSN() < log_record.GetLSN() {
				// 		page.RollbackDelete(log_record.Delete_rid, nil, nil)
				// 		page.SetLSN(log_record.GetLSN())
				// 	}
				// 	log_recovery.buffer_pool_manager.UnpinPage(log_record.Delete_rid.GetPageId(), true)
				// } else if log_record.Log_record_type == UPDATE {
				// 	page :=
				// 		access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.Update_rid.GetPageId()))
				// 	if page.GetLSN() < log_record.GetLSN() {
				// 		page.UpdateTuple(log_record.New_tuple, &log_record.Old_tuple, log_record.Update_rid, nil, nil, nil)
				// 		page.SetLSN(log_record.GetLSN())
				// 	}
				// 	log_recovery.buffer_pool_manager.UnpinPage(log_record.Update_rid.GetPageId(), true)
			} else if log_record.Log_record_type == recovery.BEGIN {
				log_recovery.active_txn[log_record.Txn_id] = log_record.Lsn
			} else if log_record.Log_record_type == recovery.COMMIT {
				delete(log_recovery.active_txn, log_record.Txn_id)
			} else if log_record.Log_record_type == recovery.NEWPAGE {
				var page_id types.PageID
				//new_page := access.CastPageAsTablePage(log_recovery.buffer_pool_manager.NewPage(&page_id, nil))
				new_page := access.CastPageAsTablePage(log_recovery.buffer_pool_manager.NewPage())
				page_id = new_page.GetPageId()
				fmt.Printf("page_id: %d\n", page_id)
				new_page.Init(page_id, log_record.Prev_page_id, nil, nil, nil)
				log_recovery.buffer_pool_manager.UnpinPage(page_id, true)
			}
			buffer_offset += log_record.Size
		}
		// incomplete log record
		fmt.Printf("buffer_offset %d\n", buffer_offset)
		file_offset += buffer_offset
	}
}

// TODO: (SDB) not ported route exists at Undo method of LogRecovery class.
//             when Log_record_type is APPLYDELETE, MARKDELETE, ROLLBACKDELETE, UPDATE

/*
*undo phase on TABLE PAGE level(table/table_page.h)
*iterate through active txn map and undo each operation
 */
func (log_recovery *LogRecovery) Undo() {
	var file_offset int
	var log_record *recovery.LogRecord
	for _, lsn := range log_recovery.active_txn {
		//lsn = it.second
		for lsn != common.InvalidLSN {
			file_offset = log_recovery.lsn_mapping[lsn]
			fmt.Printf("file_offset: %d\n", file_offset)
			(*log_recovery.disk_manager).ReadLog(log_recovery.log_buffer, int32(file_offset))
			log_recovery.DeserializeLogRecord(log_recovery.log_buffer, log_record)
			if log_record.Log_record_type == recovery.INSERT {
				page :=
					access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.Insert_rid.GetPageId()))
				fmt.Printf("insert log type, page lsn:%d, log lsn:%d", page.GetLSN(), log_record.GetLSN())
				page.ApplyDelete(log_record.Insert_rid, nil, nil)
				log_recovery.buffer_pool_manager.UnpinPage(log_record.Insert_rid.GetPageId(), true)
			} else if log_record.Log_record_type == recovery.APPLYDELETE {
				page :=
					access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.Delete_rid.GetPageId()))
				log_record.Delete_tuple.SetRID(&log_record.Delete_rid)
				page.InsertTuple(&log_record.Delete_tuple, nil, nil, nil)
				log_recovery.buffer_pool_manager.UnpinPage(log_record.Delete_rid.GetPageId(), true)
				// } else if log_record.Log_record_type == recovery.MARKDELETE {
				// 	page :=
				// 		access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.Delete_rid.GetPageId(), nil))
				// 	page.RollbackDelete(log_record.Delete_rid, nil, nil)
				// 	log_recovery.buffer_pool_manager.UnpinPage(log_record.Delete_rid.GetPageId(), true)
				// } else if log_record.Log_record_type == recovery.ROLLBACKDELETE {
				// 	page :=
				// 		access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.Delete_rid.GetPageId(), nil))
				// 	page.MarkDelete(log_record.Delete_rid, nil, nil, nil)
				// 	log_recovery.buffer_pool_manager.UnpinPage(log_record.Delete_rid.GetPageId(), true)
				// } else if log_record.Log_record_type == recovery.UPDATE {
				// 	page :=
				// 		access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.Update_rid.GetPageId(), nil))
				// 	page.UpdateTuple(log_record.Old_tuple, &log_record.New_tuple, log_record.Update_rid, nil, nil, nil)
				// 	log_recovery.buffer_pool_manager.UnpinPage(log_record.Update_rid.GetPageId(), true)
			}
			lsn = log_record.Prev_lsn
		}
	}
	log_recovery.buffer_pool_manager.FlushAllPages()
}

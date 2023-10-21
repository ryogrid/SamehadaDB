package log_recovery

import (
	"bytes"
	"encoding/binary"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"unsafe"

	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/disk"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * Read log file from disk, redo and undo.
 */
type LogRecovery struct {
	disk_manager        disk.DiskManager
	buffer_pool_manager *buffer.BufferPoolManager
	log_manager         *recovery.LogManager

	/** Maintain active transactions and its corresponding latest lsn. */
	active_txn map[types.TxnID]types.LSN
	/** Mapping the log sequence number to log file offset for undos. */
	lsn_mapping map[types.LSN]int

	offset     int32
	log_buffer []byte
}

func NewLogRecovery(disk_manager disk.DiskManager, buffer_pool_manager *buffer.BufferPoolManager, log_manager *recovery.LogManager) *LogRecovery {
	return &LogRecovery{disk_manager, buffer_pool_manager, log_manager, make(map[types.TxnID]types.LSN), make(map[types.LSN]int), 0, make([]byte, common.LogBufferSize)}
}

/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
func (log_recovery *LogRecovery) DeserializeLogRecord(data []byte, log_record *recovery.LogRecord) bool {
	if len(data) < int(recovery.HEADER_SIZE) {
		// fmt.Printf("len(data) = %d\n", len(data))
		// fmt.Println("return false point 1")
		return false
	}
	// First, unserialize the must have fields(20 bytes in total)
	record_construct_buf := new(bytes.Buffer)
	record_construct_buf.Write(data[:recovery.HEADER_SIZE])
	binary.Read(record_construct_buf, binary.LittleEndian, &(log_record.Size))
	binary.Read(record_construct_buf, binary.LittleEndian, &(log_record.Lsn))
	binary.Read(record_construct_buf, binary.LittleEndian, &(log_record.Txn_id))
	binary.Read(record_construct_buf, binary.LittleEndian, &(log_record.Prev_lsn))
	binary.Read(record_construct_buf, binary.LittleEndian, &(log_record.Log_record_type))

	if log_record.Size <= 0 {
		// fmt.Println(log_record)
		// fmt.Println("return false point 2")
		return false
	}

	pos := recovery.HEADER_SIZE
	if log_record.Log_record_type == recovery.INSERT {
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, &log_record.Insert_rid)
		pos += uint32(unsafe.Sizeof(log_record.Insert_rid))
		// we have provided serialize function for tuple class
		log_record.Insert_tuple.DeserializeFrom(data[pos:])
	} else if log_record.Log_record_type == recovery.APPLYDELETE ||
		log_record.Log_record_type == recovery.MARKDELETE ||
		log_record.Log_record_type == recovery.ROLLBACKDELETE {
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, &log_record.Delete_rid)
		pos += uint32(unsafe.Sizeof(log_record.Delete_rid))
		// we have provided serialize function for tuple class
		log_record.Delete_tuple.DeserializeFrom(data[pos:])
	} else if log_record.Log_record_type == recovery.UPDATE {
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, &log_record.Update_rid)
		pos += uint32(unsafe.Sizeof(log_record.Update_rid))
		// we have provided serialize function for tuple class
		log_record.Old_tuple.DeserializeFrom(data[pos:])
		pos += log_record.Old_tuple.Size() + uint32(tuple.TupleSizeOffsetInLogrecord)
		log_record.New_tuple.DeserializeFrom(data[pos:])
	} else if log_record.Log_record_type == recovery.NEWPAGE {
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, &log_record.Prev_page_id)
	}

	//fmt.Println(log_record)

	return true
}

/*
*redo phase on TABLE PAGE level(table/table_page.h)
*read log file from the beginning to end (you must prefetch log records into
*log buffer to reduce unnecessary I/O operations), remember to compare page's
*LSN with log_record's sequence number, and also build active_txn table &
*lsn_mapping table
* first return value: greatest LSN of log entries
* seconde return value: when redo operation occured, value is true
 */
func (log_recovery *LogRecovery) Redo(txn *access.Transaction) (types.LSN, bool) {
	greatestLSN := 0
	log_recovery.log_buffer = make([]byte, common.LogBufferSize)
	var file_offset uint32 = 0
	var readBytes uint32
	isRedoOccured := false
	for log_recovery.disk_manager.ReadLog(log_recovery.log_buffer, int32(file_offset), &readBytes) {
		var buffer_offset uint32 = 0
		var log_record recovery.LogRecord
		for log_recovery.DeserializeLogRecord(log_recovery.log_buffer[buffer_offset:readBytes], &log_record) {
			if int(log_record.Lsn) > greatestLSN {
				greatestLSN = int(log_record.Lsn)
			}
			log_recovery.active_txn[log_record.Txn_id] = log_record.Lsn
			log_recovery.lsn_mapping[log_record.Lsn] = int(file_offset + buffer_offset)
			if log_record.Log_record_type == recovery.INSERT {
				page_ :=
					access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.Insert_rid.GetPageId()))
				if page_.GetLSN() < log_record.GetLSN() {
					log_record.Insert_tuple.SetRID(&log_record.Insert_rid)
					page_.InsertTuple(&log_record.Insert_tuple, log_recovery.log_manager, nil, txn)
					page_.SetLSN(log_record.GetLSN())
					isRedoOccured = true
				}
				log_recovery.buffer_pool_manager.UnpinPage(log_record.Insert_rid.GetPageId(), true)
			} else if log_record.Log_record_type == recovery.APPLYDELETE {
				page_ :=
					access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.Delete_rid.GetPageId()))
				if page_.GetLSN() < log_record.GetLSN() {
					page_.ApplyDelete(&log_record.Delete_rid, txn, log_recovery.log_manager)
					page_.SetLSN(log_record.GetLSN())
					isRedoOccured = true
				}
				log_recovery.buffer_pool_manager.UnpinPage(log_record.Delete_rid.GetPageId(), true)
			} else if log_record.Log_record_type == recovery.MARKDELETE {
				page_ :=
					access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.Delete_rid.GetPageId()))
				if page_.GetLSN() < log_record.GetLSN() {
					page_.MarkDelete(&log_record.Delete_rid, txn, nil, log_recovery.log_manager)
					page_.SetLSN(log_record.GetLSN())
					isRedoOccured = true
				}
				log_recovery.buffer_pool_manager.UnpinPage(log_record.Delete_rid.GetPageId(), true)
			} else if log_record.Log_record_type == recovery.ROLLBACKDELETE {
				page_ :=
					access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.Delete_rid.GetPageId()))
				if page_.GetLSN() < log_record.GetLSN() {
					page_.RollbackDelete(&log_record.Delete_rid, txn, log_recovery.log_manager)
					page_.SetLSN(log_record.GetLSN())
					isRedoOccured = true
				}
				log_recovery.buffer_pool_manager.UnpinPage(log_record.Delete_rid.GetPageId(), true)
			} else if log_record.Log_record_type == recovery.UPDATE {
				page_ :=
					access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(log_record.Update_rid.GetPageId()))
				if page_.GetLSN() < log_record.GetLSN() {
					// UpdateTuple overwrites Old_tuple argument
					// but it is no problem because log_record is read from log file again in Undo phase
					page_.UpdateTuple(&log_record.New_tuple, nil, nil, &log_record.Old_tuple, &log_record.Update_rid, txn, nil, log_recovery.log_manager)
					page_.SetLSN(log_record.GetLSN())
					isRedoOccured = true
				}
				log_recovery.buffer_pool_manager.UnpinPage(log_record.Update_rid.GetPageId(), true)
			} else if log_record.Log_record_type == recovery.BEGIN {
				// fmt.Println("found BEGIN log record")
				log_recovery.active_txn[log_record.Txn_id] = log_record.Lsn
			} else if log_record.Log_record_type == recovery.COMMIT {
				// fmt.Println("found COMMIT log record")
				delete(log_recovery.active_txn, log_record.Txn_id)
			} else if log_record.Log_record_type == recovery.NEWPAGE {
				var page_id types.PageID
				new_page := access.CastPageAsTablePage(log_recovery.buffer_pool_manager.NewPage())
				page_id = new_page.GetPageId()
				// fmt.Printf("page_id: %d\n", page_id)
				new_page.Init(page_id, log_record.Prev_page_id, log_recovery.log_manager, nil, txn)
				//log_recovery.buffer_pool_manager.FlushPage(page_id)
				log_recovery.buffer_pool_manager.UnpinPage(page_id, true)
			}
			buffer_offset += log_record.Size
		}
		// incomplete log record
		// fmt.Printf("buffer_offset %d\n", buffer_offset)
		file_offset += buffer_offset
	}
	return types.LSN(greatestLSN), isRedoOccured
}

/*
*undo phase on TABLE PAGE level(table/table_page.h)
*iterate through active txn map and undo each operation
* when undo operation occured, return value becomes true
 */
func (log_recovery *LogRecovery) Undo(txn *access.Transaction) bool {
	var file_offset int
	var log_record recovery.LogRecord
	isUndoOccured := false

	// table for RID conversion
	// when rid of record is changed at UpdateTuple on this method, conversion is needed for appropriate rollback.
	RIDConvMap := make(map[page.RID]*page.RID, 0)
	convRID := func(orgRID *page.RID) (convedRID *page.RID) {
		if tmpRID, ok := RIDConvMap[*orgRID]; ok {
			//fmt.Println("Abort: RID conversion occured.")
			return tmpRID
		} else {
			return orgRID
		}
	}
	updateRIDConvMap := func(orgRID *page.RID, changedRID *page.RID) {
		RIDConvMap[*orgRID] = changedRID
	}

	// fmt.Println(log_recovery.active_txn)
	// fmt.Println(log_recovery.lsn_mapping)
	for _, lsn := range log_recovery.active_txn {
		for lsn != common.InvalidLSN {
			//fmt.Printf("lsn at Undo loop top: %d\n", lsn)
			file_offset = log_recovery.lsn_mapping[lsn]
			// fmt.Printf("file_offset: %d\n", file_offset)
			var readBytes uint32
			log_recovery.disk_manager.ReadLog(log_recovery.log_buffer, int32(file_offset), &readBytes)
			log_recovery.DeserializeLogRecord(log_recovery.log_buffer[:readBytes], &log_record)
			if log_record.Log_record_type == recovery.INSERT {
				page_ :=
					access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(convRID(&log_record.Insert_rid).GetPageId()))
				// fmt.Printf("insert log type, page lsn:%d, log lsn:%d", page.GetLSN(), log_record.GetLSN())
				page_.ApplyDelete(&log_record.Insert_rid, txn, log_recovery.log_manager)
				log_recovery.buffer_pool_manager.UnpinPage(convRID(&log_record.Insert_rid).GetPageId(), true)
				isUndoOccured = true
			} else if log_record.Log_record_type == recovery.APPLYDELETE {
				page_ :=
					access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(convRID(&log_record.Delete_rid).GetPageId()))
				log_record.Delete_tuple.SetRID(convRID(&log_record.Delete_rid))
				page_.InsertTuple(&log_record.Delete_tuple, log_recovery.log_manager, nil, txn)
				log_recovery.buffer_pool_manager.UnpinPage(convRID(&log_record.Delete_rid).GetPageId(), true)
				isUndoOccured = true
			} else if log_record.Log_record_type == recovery.MARKDELETE {
				page_ :=
					access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(convRID(&log_record.Delete_rid).GetPageId()))
				page_.RollbackDelete(convRID(&log_record.Delete_rid), txn, log_recovery.log_manager)
				log_recovery.buffer_pool_manager.UnpinPage(convRID(&log_record.Delete_rid).GetPageId(), true)
				isUndoOccured = true
			} else if log_record.Log_record_type == recovery.ROLLBACKDELETE {
				page_ :=
					access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(convRID(&log_record.Delete_rid).GetPageId()))
				page_.MarkDelete(convRID(&log_record.Delete_rid), txn, nil, log_recovery.log_manager)
				log_recovery.buffer_pool_manager.UnpinPage(convRID(&log_record.Delete_rid).GetPageId(), true)
				isUndoOccured = true
			} else if log_record.Log_record_type == recovery.UPDATE {
				var org_update_rid page.RID = *convRID(&log_record.Update_rid)
				page_ :=
					access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(org_update_rid.GetPageId()))
				is_updated, err, need_follow_tuple := page_.UpdateTuple(&log_record.Old_tuple, nil, nil, &log_record.New_tuple, convRID(&log_record.Update_rid), txn, nil, log_recovery.log_manager)

				if is_updated == false && err == access.ErrNotEnoughSpace {
					// when rid is changed case (data is move to new page)

					// first, delete original data
					page_.ApplyDelete(&org_update_rid, txn, log_recovery.log_manager)

					var new_rid *page.RID
					var err2 error
					// second, insert updated tuple to other page
					for {
						new_rid, err2 = page_.InsertTuple(need_follow_tuple, log_recovery.log_manager, nil, txn)
						if err2 == nil || err2 == access.ErrEmptyTuple {
							break
						}

						// traverse pages until find needed capacity having page or create new page
						nextPageId := page_.GetNextPageId()
						if nextPageId.IsValid() {
							nextPage := access.CastPageAsTablePage(log_recovery.buffer_pool_manager.FetchPage(nextPageId))
							log_recovery.buffer_pool_manager.UnpinPage(page_.GetPageId(), true)
							page_ = nextPage
						} else {
							p := log_recovery.buffer_pool_manager.NewPage()
							newPage := access.CastPageAsTablePage(p)
							page_.SetNextPageId(p.GetPageId())
							currentPageId := page_.GetPageId()
							newPage.Init(p.GetPageId(), currentPageId, log_recovery.log_manager, nil, txn)
							log_recovery.buffer_pool_manager.UnpinPage(page_.GetPageId(), true)
							page_ = newPage
						}
					}

					if new_rid != nil {
						updateRIDConvMap(&org_update_rid, new_rid)
					}
				}
				log_recovery.buffer_pool_manager.UnpinPage(page_.GetPageId(), true)

				isUndoOccured = true
			}
			lsn = log_record.Prev_lsn
			// fmt.Printf("lsn at Undo loop bottom: %d\n", lsn)
		}
	}
	return isUndoOccured
}

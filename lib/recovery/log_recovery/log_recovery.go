package log_recovery

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"golang.org/x/exp/maps"
	"math"
	"unsafe"

	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/recovery"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/disk"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

/**
 * Read log file from disk, redo and undo.
 */
type LogRecovery struct {
	diskManager        disk.DiskManager
	bufferPoolManager *buffer.BufferPoolManager
	logManager         *recovery.LogManager

	/** Maintain active transactions and its corresponding latest lsn. */
	activeTxn map[types.TxnID]types.LSN
	/** Mapping the log sequence number to log file offset for undos. */
	lsnMapping map[types.LSN]int

	offset     int32
	logBuffer []byte
}

func NewLogRecovery(diskManager disk.DiskManager, bufferPoolManager *buffer.BufferPoolManager, logManager *recovery.LogManager) *LogRecovery {
	return &LogRecovery{diskManager, bufferPoolManager, logManager, make(map[types.TxnID]types.LSN), make(map[types.LSN]int), 0, make([]byte, common.LogBufferSize)}
}

/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
func (logRecov *LogRecovery) DeserializeLogRecord(data []byte, logRecord *recovery.LogRecord) bool {
	if len(data) < int(recovery.HeaderSize) {
		// fmt.Printf("len(data) = %d\n", len(data))
		// fmt.Println("return false point 1")
		return false
	}
	// First, unserialize the must have fields(20 bytes in total)
	recordConstructBuf := new(bytes.Buffer)
	recordConstructBuf.Write(data[:recovery.HeaderSize])
	binary.Read(recordConstructBuf, binary.LittleEndian, &(logRecord.Size))
	binary.Read(recordConstructBuf, binary.LittleEndian, &(logRecord.Lsn))
	binary.Read(recordConstructBuf, binary.LittleEndian, &(logRecord.TxnID))
	binary.Read(recordConstructBuf, binary.LittleEndian, &(logRecord.PrevLSN))
	binary.Read(recordConstructBuf, binary.LittleEndian, &(logRecord.LogRecordType))

	if logRecord.Size <= 0 {
		// fmt.Println(logRecord)
		// fmt.Println("return false point 2")
		return false
	}

	pos := recovery.HeaderSize
	if logRecord.LogRecordType == recovery.INSERT {
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, &logRecord.InsertRID)
		pos += uint32(unsafe.Sizeof(logRecord.InsertRID))
		logRecord.InsertTuple.DeserializeFrom(data[pos:])
	} else if logRecord.LogRecordType == recovery.APPLYDELETE ||
		logRecord.LogRecordType == recovery.MARKDELETE ||
		logRecord.LogRecordType == recovery.ROLLBACKDELETE {
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, &logRecord.DeleteRID)
		pos += uint32(unsafe.Sizeof(logRecord.DeleteRID))
		logRecord.DeleteTuple.DeserializeFrom(data[pos:])
	} else if logRecord.LogRecordType == recovery.UPDATE {
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, &logRecord.UpdateRID)
		pos += uint32(unsafe.Sizeof(logRecord.UpdateRID))
		logRecord.OldTuple.DeserializeFrom(data[pos:])
		pos += logRecord.OldTuple.Size() + uint32(tuple.TupleSizeOffsetInLogrecord)
		logRecord.NewTuple.DeserializeFrom(data[pos:])
		pos += logRecord.NewTuple.Size() + uint32(tuple.TupleSizeOffsetInLogrecord)
	} else if logRecord.LogRecordType == recovery.NewTablePage {
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, &logRecord.PrevPageID)
		pos += uint32(unsafe.Sizeof(logRecord.PrevPageID))
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, &logRecord.PageID)
	} else if logRecord.LogRecordType == recovery.DeallocatePage {
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, &logRecord.DeallocatePageID)
		pos += uint32(unsafe.Sizeof(logRecord.DeallocatePageID))
	} else if logRecord.LogRecordType == recovery.ReusePage {
		binary.Read(bytes.NewBuffer(data[pos:]), binary.LittleEndian, &logRecord.ReusePageID)
		pos += uint32(unsafe.Sizeof(logRecord.ReusePageID))
	} else if logRecord.LogRecordType == recovery.GracefulShutdown {
		// do nothing
	}

	//fmt.Println(logRecord)

	return true
}

/*
*redo phase on TABLE PAGE level(table/table_page.h)
*read log file from the beginning to end (you must prefetch log records into
*log buffer to reduce unnecessary I/O operations), remember to compare page's
*LSN with logRecord's sequence number, and also build activeTxn table &
*lsnMapping table
* first return value: greatest LSN of log entries
* second return value: when undo operation is needed, value is true
* third return value: when graceful shutdown is detected, value is true
 */
func (logRecov *LogRecovery) Redo(txn *access.Transaction) (types.LSN, bool, bool) {
	greatestLSN := 0
	logRecov.logBuffer = make([]byte, common.LogBufferSize)
	var fileOffset uint32 = 0
	var readBytes uint32
	isUndoNeeded := true
	isGracefulShutdown := false
	reusablePageMap := make(map[types.PageID]bool)
	for logRecov.diskManager.ReadLog(logRecov.logBuffer, int32(fileOffset), &readBytes) {
		var bufferOffset uint32 = 0
		var logRecord recovery.LogRecord
		for logRecov.DeserializeLogRecord(logRecov.logBuffer[bufferOffset:readBytes], &logRecord) {
			if int(logRecord.Lsn) > greatestLSN {
				greatestLSN = int(logRecord.Lsn)
			}
			logRecov.activeTxn[logRecord.TxnID] = logRecord.Lsn
			logRecov.lsnMapping[logRecord.Lsn] = int(fileOffset + bufferOffset)
			if logRecord.LogRecordType == recovery.INSERT {
				pg :=
					access.CastPageAsTablePage(logRecov.bufferPoolManager.FetchPage(logRecord.InsertRID.GetPageID()))
				if pg.GetLSN() < logRecord.GetLSN() {
					logRecord.InsertTuple.SetRID(&logRecord.InsertRID)
					pg.InsertTuple(&logRecord.InsertTuple, logRecov.logManager, nil, txn)
					pg.SetLSN(logRecord.GetLSN())
				}
				logRecov.bufferPoolManager.UnpinPage(logRecord.InsertRID.GetPageID(), true)
			} else if logRecord.LogRecordType == recovery.APPLYDELETE {
				pg :=
					access.CastPageAsTablePage(logRecov.bufferPoolManager.FetchPage(logRecord.DeleteRID.GetPageID()))
				if pg.GetLSN() < logRecord.GetLSN() {
					pg.ApplyDelete(&logRecord.DeleteRID, txn, logRecov.logManager)
					pg.SetLSN(logRecord.GetLSN())
				}
				logRecov.bufferPoolManager.UnpinPage(logRecord.DeleteRID.GetPageID(), true)
			} else if logRecord.LogRecordType == recovery.MARKDELETE {
				pg :=
					access.CastPageAsTablePage(logRecov.bufferPoolManager.FetchPage(logRecord.DeleteRID.GetPageID()))
				if pg.GetLSN() < logRecord.GetLSN() {
					pg.MarkDelete(&logRecord.DeleteRID, txn, nil, logRecov.logManager)
					pg.SetLSN(logRecord.GetLSN())
				}
				logRecov.bufferPoolManager.UnpinPage(logRecord.DeleteRID.GetPageID(), true)
			} else if logRecord.LogRecordType == recovery.ROLLBACKDELETE {
				pg :=
					access.CastPageAsTablePage(logRecov.bufferPoolManager.FetchPage(logRecord.DeleteRID.GetPageID()))
				if pg.GetLSN() < logRecord.GetLSN() {
					pg.RollbackDelete(&logRecord.DeleteRID, txn, logRecov.logManager)
					pg.SetLSN(logRecord.GetLSN())
				}
				logRecov.bufferPoolManager.UnpinPage(logRecord.DeleteRID.GetPageID(), true)
			} else if logRecord.LogRecordType == recovery.UPDATE {
				pg :=
					access.CastPageAsTablePage(logRecov.bufferPoolManager.FetchPage(logRecord.UpdateRID.GetPageID()))
				if pg.GetLSN() < logRecord.GetLSN() {
					// UpdateTuple overwrites OldTuple argument
					// but it is no problem because logRecord is read from log file again in Undo phase
					pg.UpdateTuple(&logRecord.NewTuple, nil, nil, &logRecord.OldTuple, &logRecord.UpdateRID, txn, nil, logRecov.logManager, false)
					pg.SetLSN(logRecord.GetLSN())
				}
				logRecov.bufferPoolManager.UnpinPage(logRecord.UpdateRID.GetPageID(), true)
			} else if logRecord.LogRecordType == recovery.BEGIN {
				// fmt.Println("found BEGIN log record")
				logRecov.activeTxn[logRecord.TxnID] = logRecord.Lsn
			} else if logRecord.LogRecordType == recovery.COMMIT {
				// fmt.Println("found COMMIT log record")
				delete(logRecov.activeTxn, logRecord.TxnID)
			} else if logRecord.LogRecordType == recovery.NewTablePage {
				var pageID types.PageID
				//page, _ := logRecov.bufferPoolManager.NewPage()
				newPage := access.CastPageAsTablePage(logRecov.bufferPoolManager.FetchPage(logRecord.PageID))
				pageID = newPage.GetPageID()
				// fmt.Printf("pageID: %d\n", pageID)
				newPage.Init(pageID, logRecord.PrevPageID, logRecov.logManager, nil, txn, true)
				//logRecov.bufferPoolManager.FlushPage(pageID)
				logRecov.bufferPoolManager.UnpinPage(pageID, true)
			} else if logRecord.LogRecordType == recovery.DeallocatePage {
				pageID := logRecord.DeallocatePageID
				reusablePageMap[pageID] = true
			} else if logRecord.LogRecordType == recovery.ReusePage {
				pageID := logRecord.ReusePageID
				if _, ok := reusablePageMap[pageID]; ok {
					delete(reusablePageMap, pageID)
				}
			} else if logRecord.LogRecordType == recovery.GracefulShutdown {
				// undo is not needed
				isUndoNeeded = false
				isGracefulShutdown = true
			}

			bufferOffset += logRecord.Size
		}
		// incomplete log record
		// fmt.Printf("bufferOffset %d\n", bufferOffset)
		fileOffset += bufferOffset
	}

	// recovery reusable page ids
	reusablePageIDs := maps.Keys[map[types.PageID]bool](reusablePageMap)
	logRecov.bufferPoolManager.SetReusablePageIDs(reusablePageIDs)
	// delete dummy txn
	if _, ok := logRecov.activeTxn[math.MaxInt32]; ok {
		delete(logRecov.activeTxn, math.MaxInt32)
	}

	return types.LSN(greatestLSN), isUndoNeeded, isGracefulShutdown
}

/*
*undo phase on TABLE PAGE level(table/table_page.h)
*iterate through active txn map and undo each operation
* when undo operation occured, return value becomes true
 */
func (logRecov *LogRecovery) Undo(txn *access.Transaction) bool {
	var fileOffset int
	var logRecord recovery.LogRecord
	isUndoOccured := false

	for _, lsn := range logRecov.activeTxn {
		for lsn != common.InvalidLSN {
			//fmt.Printf("lsn at Undo loop top: %d\n", lsn)
			fileOffset = logRecov.lsnMapping[lsn]
			// fmt.Printf("fileOffset: %d\n", fileOffset)
			var readBytes uint32
			logRecov.diskManager.ReadLog(logRecov.logBuffer, int32(fileOffset), &readBytes)
			logRecov.DeserializeLogRecord(logRecov.logBuffer[:readBytes], &logRecord)
			if logRecord.LogRecordType == recovery.INSERT {
				pg :=
					access.CastPageAsTablePage(logRecov.bufferPoolManager.FetchPage(logRecord.InsertRID.GetPageID()))
				// fmt.Printf("insert log type, page lsn:%d, log lsn:%d", page.GetLSN(), logRecord.GetLSN())
				pg.ApplyDelete(&logRecord.InsertRID, txn, logRecov.logManager)
				logRecov.bufferPoolManager.UnpinPage(logRecord.InsertRID.GetPageID(), true)
				isUndoOccured = true
			} else if logRecord.LogRecordType == recovery.APPLYDELETE {
				pg :=
					access.CastPageAsTablePage(logRecov.bufferPoolManager.FetchPage(logRecord.DeleteRID.GetPageID()))
				logRecord.DeleteTuple.SetRID(&logRecord.DeleteRID)
				pg.InsertTuple(&logRecord.DeleteTuple, logRecov.logManager, nil, txn)
				logRecov.bufferPoolManager.UnpinPage(logRecord.DeleteRID.GetPageID(), true)
				isUndoOccured = true
			} else if logRecord.LogRecordType == recovery.MARKDELETE {
				pg :=
					access.CastPageAsTablePage(logRecov.bufferPoolManager.FetchPage(logRecord.DeleteRID.GetPageID()))
				pg.RollbackDelete(&logRecord.DeleteRID, txn, logRecov.logManager)
				logRecov.bufferPoolManager.UnpinPage(logRecord.DeleteRID.GetPageID(), true)
				isUndoOccured = true
			} else if logRecord.LogRecordType == recovery.ROLLBACKDELETE {
				pg :=
					access.CastPageAsTablePage(logRecov.bufferPoolManager.FetchPage(logRecord.DeleteRID.GetPageID()))
				pg.MarkDelete(&logRecord.DeleteRID, txn, nil, logRecov.logManager)
				logRecov.bufferPoolManager.UnpinPage(logRecord.DeleteRID.GetPageID(), true)
				isUndoOccured = true
			} else if logRecord.LogRecordType == recovery.UPDATE {
				pg :=
					access.CastPageAsTablePage(logRecov.bufferPoolManager.FetchPage(logRecord.UpdateRID.GetPageID()))
				_, err, _ := pg.UpdateTuple(&logRecord.OldTuple, nil, nil, &logRecord.NewTuple, &logRecord.UpdateRID, txn, nil, logRecov.logManager, true)
				if err != nil {
					panic(fmt.Sprintln("UpdateTuple at rollback failed! err:", err))
				}
				logRecov.bufferPoolManager.UnpinPage(pg.GetPageID(), true)
				isUndoOccured = true
			}

			lsn = logRecord.PrevLSN
			// fmt.Printf("lsn at Undo loop bottom: %d\n", lsn)
		}
	}
	return isUndoOccured
}

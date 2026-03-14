package recovery

import (
	"bytes"
	"encoding/binary"
	"sync"
	"unsafe"

	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/storage/disk"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

/**
 * LogManager maintains a separate thread that is awakened whenever the log buffer is full or whenever a timeout
 * happens. When the thread is awakened, the log buffer's content is written into the disk log file.
 */
type LogManager struct {
	offset         uint32
	logBufferLSN types.LSN
	/** The atomic counter which records the next log sequence number. */
	nextLSN types.LSN
	/** The log records before and including the persistent lsn have been written to disk. */
	persistentLSN  types.LSN
	logBuffer      []byte
	flushBuffer    []byte
	latch           common.ReaderWriterLatch
	wlogMutex      *sync.Mutex
	diskManager    *disk.DiskManager //__attribute__((__unused__));
	isEnableLogging bool
}

func NewLogManager(diskManager *disk.DiskManager) *LogManager {
	ret := new(LogManager)
	ret.nextLSN = 0
	ret.persistentLSN = common.InvalidLSN
	ret.diskManager = diskManager
	ret.logBuffer = make([]byte, common.LogBufferSize)
	ret.flushBuffer = make([]byte, common.LogBufferSize)
	ret.latch = common.NewRWLatch()
	ret.wlogMutex = new(sync.Mutex)
	ret.offset = 0
	ret.isEnableLogging = false
	return ret
}

func (logMgr *LogManager) GetNextLSN() types.LSN       { return logMgr.nextLSN }
func (logMgr *LogManager) SetNextLSN(lsnVal types.LSN) { logMgr.nextLSN = lsnVal }
func (logMgr *LogManager) GetPersistentLSN() types.LSN { return logMgr.persistentLSN }

func (logMgr *LogManager) Flush() {
	// TODO: (SDB) need fix to occur buffer swap when already running flushing?

	// For I/O efficiency, ideally flush thread should be used like below
	// https://github.com/astronaut0131/bustub/blob/master/src/recovery/logMgr.cpp#L39
	// maybe, blocking can be eliminated because txn must wait for log persistence at commit
	// https://github.com/astronaut0131/bustub/blob/master/src/concurrency/transaction_manager.cpp#L64

	logMgr.wlogMutex.Lock()
	logMgr.latch.WLock()

	lsn := logMgr.logBufferLSN
	offset := logMgr.offset
	logMgr.offset = 0

	// swap address of two buffers
	tmpP := logMgr.flushBuffer
	logMgr.flushBuffer = logMgr.logBuffer
	logMgr.logBuffer = tmpP

	logMgr.latch.WUnlock()

	// fmt.Printf("offset at Flush:%d\n", offset)
	(*logMgr.diskManager).WriteLog(logMgr.flushBuffer[:offset])

	logMgr.persistentLSN = lsn
	logMgr.wlogMutex.Unlock()
}

/*
* set enable_logging = true
* Start a separate thread to execute flush to disk operation periodically
* The flush can be triggered when the log buffer is full or buffer pool
* manager wants to force flush (it only happens when the flushed page has a
* larger LSN than persistent LSN)
 */
func (logMgr *LogManager) ActivateLogging() { logMgr.isEnableLogging = true }

/*
* Stop and join the flush thread, set enable_logging = false
 */
func (logMgr *LogManager) DeactivateLogging() { logMgr.isEnableLogging = false }

func (logMgr *LogManager) IsEnabledLogging() bool { return logMgr.isEnableLogging }

/*
* append a log record into log buffer
* return: lsn that is assigned to this log record
 */
func (logMgr *LogManager) AppendLogRecord(logRecord *LogRecord) types.LSN {
	// First, serialize the must have fields(20 bytes in total)

	logMgr.latch.WLock()
	if common.LogBufferSize-logMgr.offset < HeaderSize {
		logMgr.latch.WUnlock()
		logMgr.Flush()
		logMgr.latch.WLock()
	}

	// except DeallocatePage and ReusePage
	if logRecord.Lsn != -1 {
		logRecord.Lsn = logMgr.nextLSN
		logMgr.nextLSN += 1
	}

	headerInBytes := logRecord.GetLogHeaderData()
	copy(logMgr.logBuffer[logMgr.offset:], headerInBytes)

	if common.LogBufferSize-logMgr.offset < logRecord.Size {
		logMgr.latch.WUnlock()
		logMgr.Flush()
		logMgr.latch.WLock()
		copy(logMgr.logBuffer[logMgr.offset:], logRecord.GetLogHeaderData())
	}
	if logRecord.Lsn != -1 {
		logMgr.logBufferLSN = logRecord.Lsn
	}

	pos := logMgr.offset + HeaderSize
	logMgr.offset += logRecord.Size

	if logRecord.LogRecordType == INSERT {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, logRecord.InsertRID)
		ridInBytes := buf.Bytes()
		copy(logMgr.logBuffer[pos:], ridInBytes)
		pos += uint32(unsafe.Sizeof(logRecord.InsertRID))
		// we have provided serialize function for tuple class
		logRecord.InsertTuple.SerializeTo(logMgr.logBuffer[pos:])
	} else if logRecord.LogRecordType == APPLYDELETE ||
		logRecord.LogRecordType == MARKDELETE ||
		logRecord.LogRecordType == ROLLBACKDELETE {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, logRecord.DeleteRID)
		ridInBytes := buf.Bytes()
		copy(logMgr.logBuffer[pos:], ridInBytes)
		pos += uint32(unsafe.Sizeof(logRecord.DeleteRID))
		// we have provided serialize function for tuple class
		logRecord.DeleteTuple.SerializeTo(logMgr.logBuffer[pos:])
	} else if logRecord.LogRecordType == UPDATE {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, logRecord.UpdateRID)
		ridInBytes := buf.Bytes()
		copy(logMgr.logBuffer[pos:], ridInBytes)
		pos += uint32(unsafe.Sizeof(logRecord.UpdateRID))
		// we have provided serialize function for tuple class
		logRecord.OldTuple.SerializeTo(logMgr.logBuffer[pos:])
		pos += logRecord.OldTuple.Size() + uint32(tuple.TupleSizeOffsetInLogrecord)
		logRecord.NewTuple.SerializeTo(logMgr.logBuffer[pos:])
	} else if logRecord.LogRecordType == NewTablePage {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, logRecord.PrevPageID)
		prevPageIDInBytes := buf.Bytes()
		copy(logMgr.logBuffer[pos:], prevPageIDInBytes)
		pos += uint32(unsafe.Sizeof(logRecord.PrevPageID))
		buf2 := new(bytes.Buffer)
		binary.Write(buf2, binary.LittleEndian, logRecord.PageID)
		pageIDInBytes := buf2.Bytes()
		copy(logMgr.logBuffer[pos:], pageIDInBytes)
	} else if logRecord.LogRecordType == DeallocatePage {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, logRecord.DeallocatePageID)
		//pos += uint32(unsafe.Sizeof(logRecord.DeallocatePageID))
		pageIDInBytes := buf.Bytes()
		copy(logMgr.logBuffer[pos:], pageIDInBytes)
	} else if logRecord.LogRecordType == ReusePage {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, logRecord.ReusePageID)
		//pos += uint32(unsafe.Sizeof(logRecord.ReusePageID))
		pageIDInBytes := buf.Bytes()
		copy(logMgr.logBuffer[pos:], pageIDInBytes)
	} else if logRecord.LogRecordType == GracefulShutdown {
		// do nothing
	}

	logMgr.latch.WUnlock()
	return logRecord.Lsn
}

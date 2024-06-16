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
	// TODO: (SDB) must ensure atomicity if current locking becomes not enough
	offset uint32
	// TODO: (SDB) must ensure atomicity if current locking becomes not enough
	log_buffer_lsn types.LSN
	/** The atomic counter which records the next log sequence number. */
	// TODO: (SDB) must ensure atomicity if current locking becomes not enough
	next_lsn types.LSN
	/** The log records before and including the persistent lsn have been written to disk. */
	// TODO: (SDB) must ensure atomicity if current locking becomes not enough
	persistent_lsn  types.LSN
	log_buffer      []byte
	flush_buffer    []byte
	latch           common.ReaderWriterLatch
	wlog_mutex      *sync.Mutex
	disk_manager    *disk.DiskManager //__attribute__((__unused__));
	isEnableLogging bool
}

func NewLogManager(disk_manager *disk.DiskManager) *LogManager {
	ret := new(LogManager)
	ret.next_lsn = 0
	ret.persistent_lsn = common.InvalidLSN
	ret.disk_manager = disk_manager
	ret.log_buffer = make([]byte, common.LogBufferSize)
	ret.flush_buffer = make([]byte, common.LogBufferSize)
	ret.latch = common.NewRWLatch()
	ret.wlog_mutex = new(sync.Mutex)
	ret.offset = 0
	ret.isEnableLogging = false
	return ret
}

func (log_manager *LogManager) GetNextLSN() types.LSN       { return log_manager.next_lsn }
func (log_manager *LogManager) SetNextLSN(lsnVal types.LSN) { log_manager.next_lsn = lsnVal }
func (log_manager *LogManager) GetPersistentLSN() types.LSN { return log_manager.persistent_lsn }

func (log_manager *LogManager) Flush() {
	// TODO: (SDB) need fix to occur buffer swap when already running flushing?

	// For I/O efficiency, ideally flush thread should be used like below
	// https://github.com/astronaut0131/bustub/blob/master/src/recovery/log_manager.cpp#L39
	// maybe, blocking can be eliminated because txn must wait for log persistence at commit
	// https://github.com/astronaut0131/bustub/blob/master/src/concurrency/transaction_manager.cpp#L64

	log_manager.wlog_mutex.Lock()
	log_manager.latch.WLock()

	lsn := log_manager.log_buffer_lsn
	offset := log_manager.offset
	log_manager.offset = 0

	// swap address of two buffers
	tmp_p := log_manager.flush_buffer
	log_manager.flush_buffer = log_manager.log_buffer
	log_manager.log_buffer = tmp_p

	log_manager.latch.WUnlock()

	// fmt.Printf("offset at Flush:%d\n", offset)
	(*log_manager.disk_manager).WriteLog(log_manager.flush_buffer[:offset])

	log_manager.persistent_lsn = lsn
	log_manager.wlog_mutex.Unlock()
}

/*
* set enable_logging = true
* Start a separate thread to execute flush to disk operation periodically
* The flush can be triggered when the log buffer is full or buffer pool
* manager wants to force flush (it only happens when the flushed page has a
* larger LSN than persistent LSN)
 */
func (log_manager *LogManager) ActivateLogging() { log_manager.isEnableLogging = true }

/*
* Stop and join the flush thread, set enable_logging = false
 */
func (log_manager *LogManager) DeactivateLogging() { log_manager.isEnableLogging = false }

func (log_manager *LogManager) IsEnabledLogging() bool { return log_manager.isEnableLogging }

/*
* append a log record into log buffer
* return: lsn that is assigned to this log record
 */
func (log_manager *LogManager) AppendLogRecord(log_record *LogRecord) types.LSN {
	// First, serialize the must have fields(20 bytes in total)

	log_manager.latch.WLock()
	if common.LogBufferSize-log_manager.offset < HEADER_SIZE {
		log_manager.latch.WUnlock()
		log_manager.Flush()
		log_manager.latch.WLock()
	}

	log_record.Lsn = log_manager.next_lsn
	log_manager.next_lsn += 1
	headerInBytes := log_record.GetLogHeaderData()
	copy(log_manager.log_buffer[log_manager.offset:], headerInBytes)

	if common.LogBufferSize-log_manager.offset < log_record.Size {
		log_manager.latch.WUnlock()
		log_manager.Flush()
		log_manager.latch.WLock()
		copy(log_manager.log_buffer[log_manager.offset:], log_record.GetLogHeaderData())
	}
	log_manager.log_buffer_lsn = log_record.Lsn
	pos := log_manager.offset + HEADER_SIZE
	log_manager.offset += log_record.Size

	if log_record.Log_record_type == INSERT {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, log_record.Insert_rid)
		ridInBytes := buf.Bytes()
		copy(log_manager.log_buffer[pos:], ridInBytes)
		pos += uint32(unsafe.Sizeof(log_record.Insert_rid))
		// we have provided serialize function for tuple class
		log_record.Insert_tuple.SerializeTo(log_manager.log_buffer[pos:])
	} else if log_record.Log_record_type == APPLYDELETE ||
		log_record.Log_record_type == MARKDELETE ||
		log_record.Log_record_type == ROLLBACKDELETE {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, log_record.Delete_rid)
		ridInBytes := buf.Bytes()
		copy(log_manager.log_buffer[pos:], ridInBytes)
		pos += uint32(unsafe.Sizeof(log_record.Delete_rid))
		// we have provided serialize function for tuple class
		log_record.Delete_tuple.SerializeTo(log_manager.log_buffer[pos:])
	} else if log_record.Log_record_type == UPDATE {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, log_record.Update_rid)
		ridInBytes := buf.Bytes()
		copy(log_manager.log_buffer[pos:], ridInBytes)
		pos += uint32(unsafe.Sizeof(log_record.Update_rid))
		// we have provided serialize function for tuple class
		log_record.Old_tuple.SerializeTo(log_manager.log_buffer[pos:])
		pos += log_record.Old_tuple.Size() + uint32(tuple.TupleSizeOffsetInLogrecord)
		log_record.New_tuple.SerializeTo(log_manager.log_buffer[pos:])
	} else if log_record.Log_record_type == NEWPAGE {
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, log_record.Prev_page_id)
		pageIdInBytes := buf.Bytes()
		copy(log_manager.log_buffer[pos:], pageIdInBytes)
	}

	log_manager.latch.WUnlock()
	return log_record.Lsn
}

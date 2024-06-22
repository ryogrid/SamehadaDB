package recovery

import (
	"bytes"
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

const HEADER_SIZE uint32 = 20

type LogRecordType int32

/** The type of the log record. */
const (
	INVALID LogRecordType = iota
	INSERT
	MARKDELETE
	APPLYDELETE
	ROLLBACKDELETE
	UPDATE
	BEGIN
	COMMIT
	ABORT
	/** Creating a new page in the table heap. */
	NEW_TABLE_PAGE
	DEALLOCATE_PAGE
	REUSE_PAGE
	// this log represents last shutdown of the system is graceful
	// and this log is located at the end of log file
	GRACEFUL_SHUTDOWN
)

/**
 * For every write operation on the table page, you should write ahead a corresponding log record.
 *
 * For EACH log record, HEADER is like (5 fields in common, 20 bytes in total).
 *---------------------------------------------
 * | size | LSN | transID | prevLSN | LogType |
 *---------------------------------------------
 * For insert type log record
 *---------------------------------------------------------------
 * | HEADER | tuple_rid | tuple_size | tuple_data(char[] array) |
 *---------------------------------------------------------------
 * For delete type (including markdelete, rollbackdelete, applydelete)
 *----------------------------------------------------------------
 * | HEADER | tuple_rid | tuple_size | tuple_data(char[] array) |
 *---------------------------------------------------------------
 * For update type log record
 *-----------------------------------------------------------------------------------
 * | HEADER | tuple_rid | tuple_size | old_tuple_data | tuple_size | new_tuple_data |
 *-----------------------------------------------------------------------------------
 * For new table page type log record
 *--------------------------
 * | HEADER | prev_page_id |
 *--------------------------
 * For deallocate page type log record
 *--------------------------
 * | HEADER | page_id |
 *--------------------------
* For reuse page type log record
 *--------------------------
 * | HEADER | page_id |
 *--------------------------
* For gracefull shutdown type log record
 *--------------------------
 * | HEADER | greatest_lsn |
 *--------------------------
*/

type LogRecord struct {
	// the length of log record(for serialization, in bytes)
	Size uint32 //0
	// must have fields
	Lsn             types.LSN     //INVALID_LSN
	Txn_id          types.TxnID   //INVALID_TXN_ID
	Prev_lsn        types.LSN     //INVALID_LSN
	Log_record_type LogRecordType // {LogRecordType::INVALID}

	// case1: for delete opeartion, delete_tuple for UNDO opeartion
	Delete_rid   page.RID
	Delete_tuple tuple.Tuple

	// case2: for insert opeartion
	Insert_rid   page.RID
	Insert_tuple tuple.Tuple

	// case3: for update opeartion
	Update_rid page.RID
	Old_tuple  tuple.Tuple
	New_tuple  tuple.Tuple

	// case4: for new table page opeartion
	Prev_page_id types.PageID
	Page_id      types.PageID

	// case5: for deallocate page operation
	Deallocate_page_id types.PageID
	// note: Lsn and Prev_lsn are -1 and txn_id is math.MaxInt32
	//       and this type log is handled in redo phase only

	// case6: for reuse page operation
	Reuse_page_id types.PageID
	// note: Lsn and Prev_lsn are -1 and txn_id is math.MaxInt32
	//       and this type log is handled in redo phase only

	// case7: for graceful shutdown operation
	// nothing LSN is the greatest LSN in the log file
}

// constructor for Transaction type(BEGIN/COMMIT/ABORT)
func NewLogRecordTxn(txn_id types.TxnID, prev_lsn types.LSN, log_record_type LogRecordType) *LogRecord {
	ret := new(LogRecord)
	ret.Size = HEADER_SIZE
	ret.Txn_id = txn_id
	ret.Prev_lsn = prev_lsn
	ret.Log_record_type = log_record_type
	return ret
}

// constructor for INSERT/DELETE type
func NewLogRecordInsertDelete(txn_id types.TxnID, prev_lsn types.LSN, log_record_type LogRecordType, rid page.RID, tuple *tuple.Tuple) *LogRecord {
	ret := new(LogRecord)
	ret.Txn_id = txn_id
	ret.Prev_lsn = prev_lsn
	ret.Log_record_type = log_record_type
	if log_record_type == INSERT {
		ret.Insert_rid = rid
		ret.Insert_tuple = *tuple
	} else {
		ret.Delete_rid = rid
		ret.Delete_tuple = *tuple
	}
	// calculate log record size
	ret.Size = HEADER_SIZE + uint32(unsafe.Sizeof(rid)) + uint32(unsafe.Sizeof(int32(0))) + tuple.Size()
	return ret
}

// constructor for UPDATE type
func NewLogRecordUpdate(txn_id types.TxnID, prev_lsn types.LSN, log_record_type LogRecordType, update_rid page.RID,
	old_tuple tuple.Tuple, new_tuple tuple.Tuple) *LogRecord {
	ret := new(LogRecord)
	ret.Txn_id = txn_id
	ret.Prev_lsn = prev_lsn
	ret.Log_record_type = log_record_type
	ret.Update_rid = update_rid
	ret.Old_tuple = old_tuple
	ret.New_tuple = new_tuple
	// calculate log record size
	ret.Size = HEADER_SIZE + uint32(unsafe.Sizeof(update_rid)) + old_tuple.Size() + new_tuple.Size() + 2*uint32(unsafe.Sizeof(int32(0)))
	return ret
}

// constructor for NEW_TABLE_PAGE type
func NewLogRecordNewPage(txn_id types.TxnID, prev_lsn types.LSN, log_record_type LogRecordType, prev_page_id types.PageID, page_id types.PageID) *LogRecord {
	ret := new(LogRecord)
	ret.Size = HEADER_SIZE
	ret.Txn_id = txn_id
	ret.Prev_lsn = prev_lsn
	ret.Log_record_type = log_record_type
	ret.Prev_page_id = prev_page_id
	ret.Page_id = page_id
	// calculate log record size
	ret.Size = HEADER_SIZE + uint32(unsafe.Sizeof(prev_page_id)) + uint32(unsafe.Sizeof(page_id))
	return ret
}

func NewLogRecordDeallocatePage(page_id types.PageID) *LogRecord {
	ret := new(LogRecord)
	ret.Size = HEADER_SIZE
	ret.Txn_id = types.TxnID(math.MaxInt32)
	ret.Prev_lsn = -1
	ret.Lsn = -1
	ret.Log_record_type = DEALLOCATE_PAGE
	ret.Deallocate_page_id = page_id
	// calculate log record size
	ret.Size = HEADER_SIZE + uint32(unsafe.Sizeof(page_id))
	return ret
}

func NewLogRecordReusePage(page_id types.PageID) *LogRecord {
	ret := new(LogRecord)
	ret.Size = HEADER_SIZE
	ret.Txn_id = types.TxnID(math.MaxInt32)
	ret.Prev_lsn = -1
	ret.Lsn = -1
	ret.Log_record_type = REUSE_PAGE
	ret.Reuse_page_id = page_id
	// calculate log record size
	ret.Size = HEADER_SIZE + uint32(unsafe.Sizeof(page_id))
	return ret
}

func NewLogRecordGracefulShutdown() *LogRecord {
	ret := new(LogRecord)
	ret.Size = HEADER_SIZE
	ret.Txn_id = types.TxnID(math.MaxInt32)
	ret.Prev_lsn = -1
	ret.Lsn = math.MinInt32
	ret.Log_record_type = GRACEFUL_SHUTDOWN
	// calculate log record size
	ret.Size = HEADER_SIZE
	return ret
}

func (log_record *LogRecord) GetDeleteRID() page.RID          { return log_record.Delete_rid }
func (log_record *LogRecord) GetInserteTuple() tuple.Tuple    { return log_record.Insert_tuple }
func (log_record *LogRecord) GetInsertRID() page.RID          { return log_record.Insert_rid }
func (log_record *LogRecord) GetNewPageRecord() types.PageID  { return log_record.Prev_page_id }
func (log_record *LogRecord) GetSize() uint32                 { return log_record.Size }
func (log_record *LogRecord) GetLSN() types.LSN               { return log_record.Lsn }
func (log_record *LogRecord) GetTxnId() types.TxnID           { return log_record.Txn_id }
func (log_record *LogRecord) GetPrevLSN() types.LSN           { return log_record.Prev_lsn }
func (log_record *LogRecord) GetLogRecordType() LogRecordType { return log_record.Log_record_type }

func (log_record *LogRecord) GetLogHeaderData() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, log_record.Size)
	binary.Write(buf, binary.LittleEndian, log_record.Lsn)
	binary.Write(buf, binary.LittleEndian, log_record.Txn_id)
	binary.Write(buf, binary.LittleEndian, log_record.Prev_lsn)
	binary.Write(buf, binary.LittleEndian, log_record.Log_record_type)

	// fmt.Printf("GetLogHeaderData: %d, %d, %d, %d, %d, %d\n",
	// 	log_record.Size,
	// 	log_record.Txn_id,
	// 	log_record.Prev_lsn,
	// 	log_record.Log_record_type,
	// 	log_record.Prev_page_id,
	// 	buf.Len())
	return buf.Bytes()
}

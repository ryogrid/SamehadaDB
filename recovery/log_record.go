package recovery

import (
	"unsafe"

	"github.com/ryogrid/SamehadaDB/interfaces"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/types"
)

const HEADER_SIZE uint32 = 20

type LogRecordType int

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
	NEWPAGE
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
 * For new page type log record
 *--------------------------
 * | HEADER | prev_page_id |
 *--------------------------
 */

type LogRecord struct {
	// the length of log record(for serialization, in bytes)
	size uint32 //0
	// must have fields
	lsn             types.LSN     //INVALID_LSN
	txn_id          types.TxnID   //INVALID_TXN_ID
	prev_lsn        types.LSN     //INVALID_LSN
	log_record_type LogRecordType // {LogRecordType::INVALID}

	// case1: for delete opeartion, delete_tuple_ for UNDO opeartion
	delete_rid   *page.RID
	delete_tuple interfaces.ITuple

	// case2: for insert opeartion
	insert_rid   *page.RID
	insert_tuple interfaces.ITuple

	// case3: for update opeartion
	update_rid *page.RID
	old_tuple  interfaces.ITuple
	new_tuple  interfaces.ITuple

	// case4: for new page opeartion
	prev_page_id types.PageID //INVALID_PAGE_ID
}

// friend class LogManager;
// friend class LogRecovery;

//func NewLogRecord() *LogRecord {}

// constructor for Transaction type(BEGIN/COMMIT/ABORT)
func NewLogRecordTxn(txn_id types.TxnID, prev_lsn types.LSN, log_record_type LogRecordType) *LogRecord {
	ret := new(LogRecord)
	ret.size = HEADER_SIZE
	ret.txn_id = txn_id
	ret.prev_lsn = prev_lsn
	ret.log_record_type = log_record_type
	return ret
}

// constructor for INSERT/DELETE type
func NewLogRecordInsertDelete(txn_id types.TxnID, prev_lsn types.LSN, log_record_type LogRecordType, rid *page.RID, tuple interfaces.ITuple) *LogRecord {
	ret := new(LogRecord)
	ret.txn_id = txn_id
	ret.prev_lsn = prev_lsn
	ret.log_record_type = log_record_type
	if log_record_type == INSERT {
		ret.insert_rid = rid
		ret.insert_tuple = tuple
	} else {
		// assert(log_record_type == LogRecordType::APPLYDELETE || log_record_type == LogRecordType::MARKDELETE ||
		// 		log_record_type == LogRecordType::ROLLBACKDELETE)
		ret.delete_rid = rid
		ret.delete_tuple = tuple
	}
	// calculate log record size
	ret.size = HEADER_SIZE + uint32(unsafe.Sizeof(rid)) + uint32(unsafe.Sizeof(int32(0))) + tuple.Size()
	return ret
}

// constructor for UPDATE type
func NewLogRecordUpdate(txn_id types.TxnID, prev_lsn types.LSN, log_record_type LogRecordType, update_rid *page.RID,
	old_tuple interfaces.ITuple, new_tuple interfaces.ITuple) *LogRecord {
	ret := new(LogRecord)
	ret.txn_id = txn_id
	ret.prev_lsn = prev_lsn
	ret.log_record_type = log_record_type
	ret.update_rid = update_rid
	ret.old_tuple = old_tuple
	ret.new_tuple = new_tuple
	// calculate log record size
	ret.size = HEADER_SIZE + uint32(unsafe.Sizeof(update_rid)) + old_tuple.Size() + new_tuple.Size() + 2*uint32(unsafe.Sizeof(int32(0)))
	return ret
}

// constructor for NEWPAGE type
func NewLogRecordNewPage(txn_id types.TxnID, prev_lsn types.LSN, log_record_type LogRecordType, page_id types.PageID) *LogRecord {
	ret := new(LogRecord)
	ret.size = HEADER_SIZE
	ret.txn_id = txn_id
	ret.prev_lsn = prev_lsn
	ret.log_record_type = log_record_type
	ret.prev_page_id = page_id
	// calculate log record size
	ret.size = HEADER_SIZE + uint32(unsafe.Sizeof(page_id))
	return ret
}

func (log_record *LogRecord) GetDeleteRID() *page.RID            { return log_record.delete_rid }
func (log_record *LogRecord) GetInserteTuple() interfaces.ITuple { return log_record.insert_tuple }
func (log_record *LogRecord) GetInsertRID() *page.RID            { return log_record.insert_rid }
func (log_record *LogRecord) GetNewPageRecord() types.PageID     { return log_record.prev_page_id }
func (log_record *LogRecord) GetSize() uint32                    { return log_record.size }
func (log_record *LogRecord) GetLSN() types.LSN                  { return log_record.lsn }
func (log_record *LogRecord) GetTxnId() types.TxnID              { return log_record.txn_id }
func (log_record *LogRecord) GetPrevLSN() types.LSN              { return log_record.prev_lsn }
func (log_record *LogRecord) GetLogRecordType() LogRecordType    { return log_record.log_record_type }

// // For debug purpose
// std::string ToString() const {
// 	std::ostringstream os;
// 	os << "Log["
// 		<< "size:" << size_ << ", "
// 		<< "LSN:" << lsn_ << ", "
// 		<< "transID:" << txn_id_ << ", "
// 		<< "prevLSN:" << prev_lsn_ << ", "
// 		<< "LogType:" << static_cast<int>(log_record_type_) << "]";

// 	return os.str();
// }

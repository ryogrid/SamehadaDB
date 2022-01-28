package recovery

const HEADER_SIZE uint32 = 20

type LogRecordType int

/** The type of the log record. */
const {
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
}

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
	lsn LSN //INVALID_LSN
	txn_id TxnID //INVALID_TXN_ID
	prev_lsn LSN //INVALID_LSN
	log_record_type LogRecordType // {LogRecordType::INVALID}

	// case1: for delete opeartion, delete_tuple_ for UNDO opeartion
	delete_rid RID
	delete_tuple Tuple

	// case2: for insert opeartion
	insert_rid RID
	insert_tuple Tuple

	// case3: for update opeartion
	update_rid RID
	pold_tuple Tuple
	new_tuple Tuple

	// case4: for new page opeartion
	prev_page_id PageID //INVALID_PAGE_ID
}
	// friend class LogManager;
	// friend class LogRecovery;

func NewLogRecord() *LogRecord {}

// constructor for Transaction type(BEGIN/COMMIT/ABORT)
func NewLogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType log_record_type) *LogRecord {
	size := HEADER_SIZE
	txn_id := txn_id
	prev_lsn(prev_lsn)
	log_record_type(log_record_type)
}

// constructor for INSERT/DELETE type
func NewLogRecord(txn_id TxnID, prev_lsn LSN, log_record_type LogRecordType, const rid *RID, const tuple *Tuple) *LogRecord {
	txn_id(txn_id)
	prev_lsn(prev_lsn)
	log_record_type(log_record_type)
	if (log_record_type == LogRecordType::INSERT) {
		insert_rid = rid
		insert_tuple = tuple
	} else {
		assert(log_record_type == LogRecordType::APPLYDELETE || log_record_type == LogRecordType::MARKDELETE ||
				log_record_type == LogRecordType::ROLLBACKDELETE)
		delete_rid = rid
		delete_tuple = tuple;
	}
	// calculate log record size
	size = HEADER_SIZE + sizeof(RID) + sizeof(int32_t) + tuple.GetLength()
}

// constructor for UPDATE type
func NewLogRecord(txn_id_t TxnID, prev_lsn LSN, log_record_type LogRecordType, const update_rid *RID,
			const old_tuple *Tuple, const new_tuple *Tuple) *LogRecord
{
	txn_id(txn_id)
	prev_lsn(prev_lsn)
	log_record_type(log_record_type)
	update_rid(update_rid)
	old_tuple(old_tuple)
	new_tuple(new_tuple)
	// calculate log record size
	size = HEADER_SIZE + sizeof(RID) + old_tuple.GetLength() + new_tuple.GetLength() + 2 * sizeof(int32_t)
}

// constructor for NEWPAGE type
func NewLogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType log_record_type, page_id_t page_id) *LogRecord
{
	size_(HEADER_SIZE),
	txn_id_(txn_id),
	prev_lsn_(prev_lsn),
	log_record_type_(log_record_type),
	prev_page_id_(page_id)
	// calculate log record size
	size = HEADER_SIZE + sizeof(page_id_t);
}

func (log_record *LogRecord) GetDeleteRID() RID { return log_record.delete_rid }
func (log_record *LogRecord) GetInserteTuple() Tuple { return log_record.insert_tuple }
func (log_record *LogRecord) GetInsertRID() RID { return log_record.insert_rid }
func (log_record *LogRecord) GetNewPageRecord() PageID { return log_record.prev_page_id }
func (log_record *LogRecord) GetSize() uint32 { return log_record.size }
func (log_record *LogRecord) GetLSN() LSN { return log_record.lsn }
func (log_record *LogRecord) GetTxnId() TxnID { return log_record.txn_id }
func (log_record *LogRecord) GetPrevLSN() LSN { return log_record.prev_lsn }
func (log_record *LogRecord) GetLogRecordType() LogRecordType { return log_record.log_record_type }

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

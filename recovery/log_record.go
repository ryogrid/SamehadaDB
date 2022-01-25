package recovery

/** The type of the log record. */
enum class LogRecordType {
	INVALID = 0,
	INSERT,
	MARKDELETE,
	APPLYDELETE,
	ROLLBACKDELETE,
	UPDATE,
	BEGIN,
	COMMIT,
	ABORT,
	/** Creating a new page in the table heap. */
	NEWPAGE,
};

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
	size{0} uint32
	// must have fields
	lsn{INVALID_LSN} LSN
	txn_id{INVALID_TXN_ID} TxnID
	prev_lsn{INVALID_LSN} LSN
	log_record_type{LogRecordType::INVALID} LogRecordType

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
	prev_page_id_{INVALID_PAGE_ID} PageID;
	static const int HEADER_SIZE = 20;	
}
	// friend class LogManager;
	// friend class LogRecovery;

	Nwe() *LogRecord {}

	// constructor for Transaction type(BEGIN/COMMIT/ABORT)
	New(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType log_record_type) *LogRecord {
		size(HEADER_SIZE)
		txn_id(txn_id)
		prev_lsn(prev_lsn)
		log_record_type(log_record_type)
	}

	// constructor for INSERT/DELETE type
	New(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType log_record_type, const RID &rid, const Tuple &tuple) *LogRecord {
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
	  size_ = HEADER_SIZE + sizeof(RID) + sizeof(int32_t) + tuple.GetLength()
	}

	// constructor for UPDATE type
	LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType log_record_type, const RID &update_rid,
			  const Tuple &old_tuple, const Tuple &new_tuple)
		: txn_id_(txn_id),
		  prev_lsn_(prev_lsn),
		  log_record_type_(log_record_type),
		  update_rid_(update_rid),
		  old_tuple_(old_tuple),
		  new_tuple_(new_tuple) {
	  // calculate log record size
	  size_ = HEADER_SIZE + sizeof(RID) + old_tuple.GetLength() + new_tuple.GetLength() + 2 * sizeof(int32_t);
	}

	// constructor for NEWPAGE type
	LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType log_record_type, page_id_t page_id)
		: size_(HEADER_SIZE),
		  txn_id_(txn_id),
		  prev_lsn_(prev_lsn),
		  log_record_type_(log_record_type),
		  prev_page_id_(page_id) {
	  // calculate log record size
	  size_ = HEADER_SIZE + sizeof(page_id_t);
	}

	~LogRecord() = default;

	RID &GetDeleteRID() { return delete_rid_; }
	Tuple &GetInserteTuple() { return insert_tuple_; }

	RID &GetInsertRID() { return insert_rid_; }

	page_id_t GetNewPageRecord() { return prev_page_id_; }

	int32_t GetSize() { return size_; }

	lsn_t GetLSN() { return lsn_; }
	txn_id_t GetTxnId() { return txn_id_; }

	lsn_t GetPrevLSN() { return prev_lsn_; }

	LogRecordType &GetLogRecordType() { return log_record_type_; }

	// For debug purpose
	std::string ToString() const {
	  std::ostringstream os;
	  os << "Log["
		 << "size:" << size_ << ", "
		 << "LSN:" << lsn_ << ", "
		 << "transID:" << txn_id_ << ", "
		 << "prevLSN:" << prev_lsn_ << ", "
		 << "LogType:" << static_cast<int>(log_record_type_) << "]";

	  return os.str();
	}


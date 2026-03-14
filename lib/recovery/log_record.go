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

const HeaderSize uint32 = 20

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
	NewTablePage
	DeallocatePage
	ReusePage
	// this log represents last shutdown of the system is graceful
	// and this log is located at the end of log file
	GracefulShutdown
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
 * | HEADER | prevPageID |
 *--------------------------
 * For deallocate page type log record
 *--------------------------
 * | HEADER | pageID |
 *--------------------------
* For reuse page type log record
 *--------------------------
 * | HEADER | pageID |
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
	TxnID          types.TxnID   //INVALID_TXN_ID
	PrevLSN        types.LSN     //INVALID_LSN
	LogRecordType LogRecordType // {LogRecordType::INVALID}

	// case1: for delete opeartion, delete_tuple for UNDO opeartion
	DeleteRID   page.RID
	DeleteTuple tuple.Tuple

	// case2: for insert opeartion
	InsertRID   page.RID
	InsertTuple tuple.Tuple

	// case3: for update opeartion
	UpdateRID page.RID
	OldTuple  tuple.Tuple
	NewTuple  tuple.Tuple

	// case4: for new table page opeartion
	PrevPageID types.PageID
	PageID      types.PageID

	// case5: for deallocate page operation
	DeallocatePageID types.PageID
	// note: Lsn and PrevLSN are -1 and txnID is math.MaxInt32
	//       and this type log is handled in redo phase only

	// case6: for reuse page operation
	ReusePageID types.PageID
	// note: Lsn and PrevLSN are -1 and txnID is math.MaxInt32
	//       and this type log is handled in redo phase only

	// case7: for graceful shutdown operation
	// nothing LSN is the greatest LSN in the log file
}

// constructor for Transaction type(BEGIN/COMMIT/ABORT)
func NewLogRecordTxn(txnID types.TxnID, prevLSN types.LSN, logRecordType LogRecordType) *LogRecord {
	ret := new(LogRecord)
	ret.Size = HeaderSize
	ret.TxnID = txnID
	ret.PrevLSN = prevLSN
	ret.LogRecordType = logRecordType
	return ret
}

// constructor for INSERT/DELETE type
func NewLogRecordInsertDelete(txnID types.TxnID, prevLSN types.LSN, logRecordType LogRecordType, rid page.RID, tuple *tuple.Tuple) *LogRecord {
	ret := new(LogRecord)
	ret.TxnID = txnID
	ret.PrevLSN = prevLSN
	ret.LogRecordType = logRecordType
	if logRecordType == INSERT {
		ret.InsertRID = rid
		ret.InsertTuple = *tuple
	} else {
		ret.DeleteRID = rid
		ret.DeleteTuple = *tuple
	}
	// calculate log record size
	ret.Size = HeaderSize + uint32(unsafe.Sizeof(rid)) + uint32(unsafe.Sizeof(int32(0))) + tuple.Size()
	return ret
}

// constructor for UPDATE type
func NewLogRecordUpdate(txnID types.TxnID, prevLSN types.LSN, logRecordType LogRecordType, updateRID page.RID,
	oldTuple tuple.Tuple, newTuple tuple.Tuple) *LogRecord {
	ret := new(LogRecord)
	ret.TxnID = txnID
	ret.PrevLSN = prevLSN
	ret.LogRecordType = logRecordType
	ret.UpdateRID = updateRID
	ret.OldTuple = oldTuple
	ret.NewTuple = newTuple
	// calculate log record size
	ret.Size = HeaderSize + uint32(unsafe.Sizeof(updateRID)) + oldTuple.Size() + newTuple.Size() + 2*uint32(unsafe.Sizeof(int32(0)))
	return ret
}

// constructor for NewTablePage type
func NewLogRecordNewPage(txnID types.TxnID, prevLSN types.LSN, logRecordType LogRecordType, prevPageID types.PageID, pageID types.PageID) *LogRecord {
	ret := new(LogRecord)
	ret.Size = HeaderSize
	ret.TxnID = txnID
	ret.PrevLSN = prevLSN
	ret.LogRecordType = logRecordType
	ret.PrevPageID = prevPageID
	ret.PageID = pageID
	// calculate log record size
	ret.Size = HeaderSize + uint32(unsafe.Sizeof(prevPageID)) + uint32(unsafe.Sizeof(pageID))
	return ret
}

func NewLogRecordDeallocatePage(pageID types.PageID) *LogRecord {
	ret := new(LogRecord)
	ret.Size = HeaderSize
	ret.TxnID = types.TxnID(math.MaxInt32)
	ret.PrevLSN = -1
	ret.Lsn = -1
	ret.LogRecordType = DeallocatePage
	ret.DeallocatePageID = pageID
	// calculate log record size
	ret.Size = HeaderSize + uint32(unsafe.Sizeof(pageID))
	return ret
}

func NewLogRecordReusePage(pageID types.PageID) *LogRecord {
	ret := new(LogRecord)
	ret.Size = HeaderSize
	ret.TxnID = types.TxnID(math.MaxInt32)
	ret.PrevLSN = -1
	ret.Lsn = -1
	ret.LogRecordType = ReusePage
	ret.ReusePageID = pageID
	// calculate log record size
	ret.Size = HeaderSize + uint32(unsafe.Sizeof(pageID))
	return ret
}

func NewLogRecordGracefulShutdown() *LogRecord {
	ret := new(LogRecord)
	ret.Size = HeaderSize
	ret.TxnID = types.TxnID(math.MaxInt32)
	ret.PrevLSN = -1
	ret.Lsn = math.MinInt32
	ret.LogRecordType = GracefulShutdown
	// calculate log record size
	ret.Size = HeaderSize
	return ret
}

func (lr *LogRecord) GetDeleteRID() page.RID          { return lr.DeleteRID }
func (lr *LogRecord) GetInserteTuple() tuple.Tuple    { return lr.InsertTuple }
func (lr *LogRecord) GetInsertRID() page.RID          { return lr.InsertRID }
func (lr *LogRecord) GetNewPageRecord() types.PageID  { return lr.PrevPageID }
func (lr *LogRecord) GetSize() uint32                 { return lr.Size }
func (lr *LogRecord) GetLSN() types.LSN               { return lr.Lsn }
func (lr *LogRecord) GetTxnID() types.TxnID           { return lr.TxnID }
func (lr *LogRecord) GetPrevLSN() types.LSN           { return lr.PrevLSN }
func (lr *LogRecord) GetLogRecordType() LogRecordType { return lr.LogRecordType }

func (lr *LogRecord) GetLogHeaderData() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, lr.Size)
	binary.Write(buf, binary.LittleEndian, lr.Lsn)
	binary.Write(buf, binary.LittleEndian, lr.TxnID)
	binary.Write(buf, binary.LittleEndian, lr.PrevLSN)
	binary.Write(buf, binary.LittleEndian, lr.LogRecordType)

	// fmt.Printf("GetLogHeaderData: %d, %d, %d, %d, %d, %d\n",
	// 	lr.Size,
	// 	lr.TxnID,
	// 	lr.PrevLSN,
	// 	lr.LogRecordType,
	// 	lr.PrevPageID,
	// 	buf.Len())
	return buf.Bytes()
}

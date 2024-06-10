// this code is from https://github.com/pzhzqt/goostub
// there is license and copyright notice in licenses/goostub dir

package common

import (
	"sync"
	"time"
)

var LogTimeout time.Duration

const EnableDebug bool = false //true

// use on memory virtual storage or not
const EnableOnMemStorage = true

// when this is true, virtual storage use is suppressed
// for test case which can't work with virtual storage
var TempSuppressOnMemStorage = false
var TempSuppressOnMemStorageMutex sync.Mutex

// TODO: for debugging
var NewRIDAtNormal = false
var NewRIDAtRollback = false

const (
	// invalid page id
	InvalidPageID = -1
	// invalid transaction id
	InvalidTxnID = -1
	// invalid log sequence number
	InvalidLSN = -1
	// the header page id
	HeaderPageID = 0
	// size of a data page in byte
	PageSize                     = 4096 //1024  //512
	BufferPoolMaxFrameNumForTest = 500  //4000 //32
	// number for calculate log buffer size (number of page size)
	LogBufferSizeBase = 128
	// size of a log buffer in byte
	LogBufferSize = (LogBufferSizeBase + 1) * PageSize
	// size of hash bucket
	BucketSizeOfHashIndex = 10
	// probability used for determin node level on SkipList
	SkipListProb         = 0.5                             //0.25
	ActiveLogKindSetting = INFO | NOT_ABORABLE_TXN_FEATURE //| COMMIT_ABORT_HANDLE_INFO | NOT_ABORABLE_TXN_FEATURE | DEBUGGING | RDB_OP_FUNC_CALL // | DEBUG_INFO  //| BUFFER_INTERNAL_STATE //| DEBUGGING | DEBUG_INFO //| PIN_COUNT_ASSERT //DEBUG_INFO_DETAIL  //DEBUGGING
	KernelThreadNum      = 24
	MaxTxnThreadNum      = KernelThreadNum * 1
)

type TxnID int32        // transaction id type
type SlotOffset uintptr // slot offset type

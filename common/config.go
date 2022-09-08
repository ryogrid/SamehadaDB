// this code is from https://github.com/pzhzqt/goostub
// there is license and copyright notice in licenses/goostub dir

package common

import "time"

var LogTimeout time.Duration

var EnableLogging bool = false //true
const EnableDebug bool = false //true
const EnableOnMemStorage = false

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
	PageSize                     = 4096
	BufferPoolMaxFrameNumForTest = 32
	// number for calculate log buffer size (number of page size)
	LogBufferSizeBase = 32
	// size of a log buffer in byte
	LogBufferSize = ((LogBufferSizeBase + 1) * PageSize)
	// size of hash bucket
	BucketSizeOfHashIndex = 10
	// probability used for determin node level on SkipList
	SkipListProb    = 0.5  //0.25
	LogLevelSetting = INFO //DEBUGGING //DEBUG_INFO
)

type TxnID int32        // transaction id type
type SlotOffset uintptr // slot offset type

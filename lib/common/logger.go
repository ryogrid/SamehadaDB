package common

import "fmt"

type LogLevel int32

const (
	DEBUG_INFO_DETAIL        LogLevel = 1
	DEBUG_INFO                        = 2
	CACHE_OUT_IN_INFO                 = 2 << 1
	RDB_OP_FUNC_CALL                  = 2 << 2 // print several info at core functions (ex: CRUD at TableHeap and SkipList/SkipListBlockPage)
	BUFFER_INTERNAL_STATE             = 2 << 3 // print internal state of buffer of BufferPoolManager
	PIN_COUNT_ASSERT                  = 2 << 4
	COMMIT_ABORT_HANDLE_INFO          = 2 << 5
	NOT_ABORABLE_TXN_FEATURE          = 2 << 6
	DEBUGGING                         = 2 << 7 // print debug info for a debugging period (not permanently used)
	INFO                              = 2 << 8
	WARN                              = 2 << 9
	ERROR                             = 2 << 10
	FATAL                             = 2 << 11
)

func ShPrintf(logLevel LogLevel, fmtStl string, a ...interface{}) {
	if logLevel&ActiveLogKindSetting > 0 {
		fmt.Printf(fmtStl, a...)
	}
}

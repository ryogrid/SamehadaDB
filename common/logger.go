package common

import "fmt"

type LogLevel int32

const (
	DEBUG_INFO_DETAIL     LogLevel = 1
	DEBUG_INFO                     = 2
	RDB_OP_FUNC_CALL               = 2 << 1 // print several info at core functions (ex: CRUD at TableHeap and SkipList/SkipListBlockPage)
	BUFFER_INTERNAL_STATE          = 2 << 2 // print internal state of buffer of BufferPoolManager
	PIN_COUNT_ASSERT               = 2 << 3
	DEBUGGING                      = 2 << 4 // print debug info for a debugging period (not permanently used)
	INFO                           = 2 << 5
	WARN                           = 2 << 6
	ERROR                          = 2 << 7
	FATAL                          = 2 << 8
)

func ShPrintf(logLevel LogLevel, fmtStl string, a ...interface{}) {
	if logLevel&ActiveLogKindSetting > 0 {
		fmt.Printf(fmtStl, a...)
	}
}

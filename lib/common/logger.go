package common

import "fmt"

type LogLevel int32

const (
	DebugInfoDetail        LogLevel = 1
	DebugInfo                        = 2
	CacheOutInInfo                 = 2 << 1
	RDBOpFuncCall                  = 2 << 2 // print several info at core functions (ex: CRUD at TableHeap and SkipList/SkipListBlockPage)
	BufferInternalState             = 2 << 3 // print internal state of buffer of BufferPoolManager
	PinCountAssert                  = 2 << 4
	CommitAbortHandleInfo          = 2 << 5
	NotAborableTxnFeature          = 2 << 6
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

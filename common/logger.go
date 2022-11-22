package common

import "fmt"

type LogLevel int32

const (
	DEBUG_INFO_DETAIL LogLevel = 1
	DEBUG_INFO                 = 2
	RDB_OP_FUNC_CALL           = 4
	DEBUGGING                  = 8
	PIN_COUNT_ASSERT           = 16
	INFO                       = 32
	WARN                       = 64
	ERROR                      = 128
	FATAL                      = 256
)

func ShPrintf(logLevel LogLevel, fmtStl string, a ...interface{}) {
	if logLevel&LogLevelSetting > 0 {
		fmt.Printf(fmtStl, a...)
	}
}

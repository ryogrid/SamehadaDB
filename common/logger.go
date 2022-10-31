package common

import "fmt"

type LogLevel int32

const (
	DEBUG_INFO_DETAIL LogLevel = 1
	DEBUG_INFO                 = 2
	RDB_OP_FUNC_CALL           = 4
	DEBUGGING                  = 8
	INFO                       = 16
	WARN                       = 32
	ERROR                      = 64
	FATAL                      = 128
)

func ShPrintf(logLevel LogLevel, fmtStl string, a ...interface{}) {
	if logLevel&LogLevelSetting > 0 {
		fmt.Printf(fmtStl, a...)
	}
}

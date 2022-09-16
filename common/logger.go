package common

import "fmt"

type LogLevel int32

const (
	DEBUG_INFO_DETAIL LogLevel = iota
	DEBUG_INFO
	DEBUGGING
	INFO
	WARN
	ERROR
	FATAL
)

func ShPrintf(logLevel LogLevel, fmtStl string, a ...interface{}) {
	if logLevel >= LogLevelSetting {
		fmt.Printf(fmtStl, a...)
	}
}

package common

import "fmt"

type LogLevel int32

const (
	DEBUG LogLevel = iota
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

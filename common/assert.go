package common

import (
	"github.com/devlights/gomy/output"
	"runtime"
	"sync"
)

func SH_Assert(condition bool, msg string) {
	if !condition {
		panic(msg)
	}
}

type SH_Mutex struct {
	mutex    *sync.Mutex
	isLocked bool
}

func NewSH_Mutex() *SH_Mutex {
	return &SH_Mutex{new(sync.Mutex), false}
}
func (m *SH_Mutex) Lock() {
	SH_Assert(!m.isLocked, "Mutex is already locked")
	m.mutex.Lock()
	m.isLocked = true
}

func (m *SH_Mutex) Unlock() {
	SH_Assert(m.isLocked, "Mutex is not locked")
	m.mutex.Unlock()
	m.isLocked = false
}

// REFERENCES
//   - https://pkg.go.dev/runtime#Stack
//   - https://stackoverflow.com/questions/19094099/how-to-dump-goroutine-stacktraces
func RuntimeStack() error {
	// channels
	var (
		//chSingle = make(chan []byte, 1)
		chAll = make(chan []byte, 1)
	)

	// funcs
	var (
		getStack = func(all bool) []byte {
			// From src/runtime/debug/stack.go
			var (
				buf = make([]byte, 1024)
			)

			for {
				n := runtime.Stack(buf, all)
				if n < len(buf) {
					return buf[:n]
				}
				buf = make([]byte, 2*len(buf))
			}
		}
	)

	////current goroutin only
	//go func(ch chan<- []byte) {
	//	defer close(ch)
	//	ch <- getStack(false)
	//}(chSingle)

	// all goroutin
	go func(ch chan<- []byte) {
		defer close(ch)
		ch <- getStack(true)
	}(chAll)

	//// result of runtime.Stack(false)
	//for v := range chSingle {
	//	output.Stdoutl("=== stack-single", string(v))
	//}

	// result of runtime.Stack(true)
	for v := range chAll {
		output.Stdoutl("=== stack-all   ", string(v))
	}

	return nil
}

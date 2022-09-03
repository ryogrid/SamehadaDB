// this code is from https://github.com/pzhzqt/goostub
// there is license and copyright notice in licenses/goostub dir

package common

import (
	"fmt"
	"math"
	"sync"
)

type ReaderWriterLatch interface {
	WLock()
	WUnlock()
	RLock()
	RUnlock()
}

type readerWriterLatch struct {
	mutex *sync.RWMutex
}

const (
	MaxReaders = math.MaxUint32
)

func NewRWLatch() ReaderWriterLatch {
	latch := readerWriterLatch{}
	latch.mutex = new(sync.RWMutex)

	return &latch
}

func (l *readerWriterLatch) WLock() {
	l.mutex.Lock()
}

func (l *readerWriterLatch) WUnlock() {
	l.mutex.Unlock()
}

func (l *readerWriterLatch) RLock() {
	l.mutex.RLock()
}

func (l *readerWriterLatch) RUnlock() {
	l.mutex.RUnlock()
}

// for debug of cuncurrent code on single thread running
type readerWriterLatchDummy struct {
	readerCnt int32
	writerCnt int32
}

func NewRWLatchDummy() ReaderWriterLatch {
	latch := readerWriterLatchDummy{0, 0}

	return &latch
}

func (l *readerWriterLatchDummy) WLock() {
	l.writerCnt++

	if l.writerCnt != 1 {
		fmt.Printf("readerCnt: %d, writerCnt: %d\n", l.readerCnt, l.writerCnt)
		panic("double Write Lock!")
	}
}

func (l *readerWriterLatchDummy) WUnlock() {
	l.writerCnt--

	if l.writerCnt != 0 {
		fmt.Printf("readerCnt: %d, writerCnt: %d\n", l.readerCnt, l.writerCnt)
		panic("double Write Unlock!")
	}
}

func (l *readerWriterLatchDummy) RLock() {
	l.readerCnt++

	if l.readerCnt != 1 {
		fmt.Printf("readerCnt: %d, writerCnt: %d\n", l.readerCnt, l.writerCnt)
		panic("double Reader Lock!")
	}

}

func (l *readerWriterLatchDummy) RUnlock() {
	l.readerCnt--

	if l.readerCnt != 0 {
		fmt.Printf("readerCnt: %d, writerCnt: %d\n", l.readerCnt, l.writerCnt)
		panic("double Reader Unlock!")
	}
}

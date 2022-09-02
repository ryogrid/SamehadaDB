// this code is from https://github.com/pzhzqt/goostub
// there is license and copyright notice in licenses/goostub dir

package common

import (
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
	SH_Assert(l.writerCnt == 1, "double Write Lock!")
}

func (l *readerWriterLatchDummy) WUnlock() {
	l.writerCnt--
	SH_Assert(l.writerCnt == 0, "double Write Unlock!")
}

func (l *readerWriterLatchDummy) RLock() {
	l.readerCnt++
	SH_Assert(l.readerCnt == 1, "double Reader Lock!")
}

func (l *readerWriterLatchDummy) RUnlock() {
	l.readerCnt--
	SH_Assert(l.readerCnt == 0, "double Reader Unlock!")
}

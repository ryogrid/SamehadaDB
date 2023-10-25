// this code is from https://github.com/pzhzqt/goostub
// there is license and copyright notice in licenses/goostub dir

package common

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
)

type ReaderWriterLatch interface {
	WLock()
	WUnlock()
	RLock()
	RUnlock()
	PrintDebugInfo()
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

func (l *readerWriterLatch) PrintDebugInfo() {
	//do nothing
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
		panic("double Write WLock!")
	}
}

func (l *readerWriterLatchDummy) WUnlock() {
	l.writerCnt--

	if l.writerCnt != 0 {
		fmt.Printf("readerCnt: %d, writerCnt: %d\n", l.readerCnt, l.writerCnt)
		panic("double Write WUnlock!")
	}
}

func (l *readerWriterLatchDummy) RLock() {
	l.readerCnt++

	if l.readerCnt != 1 {
		fmt.Printf("readerCnt: %d, writerCnt: %d\n", l.readerCnt, l.writerCnt)
		panic("double Reader WLock!")
	}

}

func (l *readerWriterLatchDummy) RUnlock() {
	l.readerCnt--

	if l.readerCnt != 0 {
		fmt.Printf("readerCnt: %d, writerCnt: %d\n", l.readerCnt, l.writerCnt)
		panic("double Reader WUnlock!")
	}
}

func (l *readerWriterLatchDummy) PrintDebugInfo() {
	//do nothing
}

type readerWriterLatchDebug struct {
	mutex     *sync.RWMutex
	readerCnt int32
	writerCnt int32
}

func NewRWLatchDebug() ReaderWriterLatch {
	latch := readerWriterLatchDebug{new(sync.RWMutex), 0, 0}

	return &latch
}

func (l *readerWriterLatchDebug) WLock() {
	atomic.AddInt32(&l.writerCnt, 1)
	//l.writerCnt++
	fmt.Printf("WLock: readerCnt=%d, writerCnt=%d\n", l.readerCnt, l.writerCnt)

	l.mutex.Lock()
}

func (l *readerWriterLatchDebug) WUnlock() {
	atomic.AddInt32(&l.writerCnt, -1)
	//l.writerCnt--
	fmt.Printf("WUnlock: readerCnt=%d, writerCnt=%d\n", l.readerCnt, l.writerCnt)

	l.mutex.Unlock()
}

func (l *readerWriterLatchDebug) RLock() {
	atomic.AddInt32(&l.readerCnt, 1)
	//l.readerCnt++
	fmt.Printf("RLock: readerCnt=%d, writerCnt=%d\n", l.readerCnt, l.writerCnt)

	l.mutex.RLock()
}

func (l *readerWriterLatchDebug) RUnlock() {
	atomic.AddInt32(&l.readerCnt, -1)
	//l.readerCnt--
	fmt.Printf("RUnlock: readerCnt=%d, writerCnt=%d\n", l.readerCnt, l.writerCnt)

	l.mutex.RUnlock()
}

func (l *readerWriterLatchDebug) PrintDebugInfo() {
	fmt.Printf("PrintDebugInfo: readerCnt=%d, writerCnt=%d\n", l.readerCnt, l.writerCnt)
}

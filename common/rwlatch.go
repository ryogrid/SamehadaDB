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

/**
 * Reader-Writer latch backed by sync.Mutex and sync.Cond
 */
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
	//SH_Assert(!l.writerEntered, "Writer is already locked")

	l.mutex.Lock()
}

func (l *readerWriterLatch) WUnlock() {
	//SH_Assert(l.writerEntered, "Writer is not locked")
	l.mutex.Unlock()
}

func (l *readerWriterLatch) RLock() {
	// SH_Assert(!l.writerEntered, "Writer is already locked")
	// SH_Assert(l.readerCount == 0, "Reader is already locked")
	l.mutex.RLock()
}

func (l *readerWriterLatch) RUnlock() {
	// SH_Assert(l.readerCount != 0, "Reader is not locked")

	l.mutex.RUnlock()
}

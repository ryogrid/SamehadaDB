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
	mutex         *sync.Mutex
	writer        *sync.Cond
	reader        *sync.Cond
	readerCount   uint32
	writerEntered bool
}

const (
	MaxReaders = math.MaxUint32
)

func NewRWLatch() ReaderWriterLatch {
	latch := readerWriterLatch{}

	latch.mutex = new(sync.Mutex)
	latch.reader = sync.NewCond(latch.mutex)
	latch.writer = sync.NewCond(latch.mutex)

	latch.readerCount = 0
	latch.writerEntered = false

	return &latch
}

func (l *readerWriterLatch) WLock() {
	// // TODO: (SDB) Assert are for debug
	// SH_Assert(!l.writerEntered, "Writer is already locked")
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// only one is allowed to write
	for l.writerEntered {
		l.reader.Wait()
	}

	l.writerEntered = true

	// wait for readers to finish
	for l.readerCount > 0 {
		l.writer.Wait()
	}
}

func (l *readerWriterLatch) WUnlock() {
	// // TODO: (SDB) Assert are for debug
	// SH_Assert(l.writerEntered, "Writer is not locked")
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.writerEntered = false
	l.reader.Broadcast()
}

func (l *readerWriterLatch) RLock() {
	// // TODO: (SDB) Asserts are for debug
	// SH_Assert(!l.writerEntered, "Writer is already locked")
	// SH_Assert(l.readerCount == 0, "Reader is already locked")
	l.mutex.Lock()
	defer l.mutex.Unlock()

	for l.writerEntered || l.readerCount == MaxReaders {
		l.reader.Wait()
	}

	l.readerCount++
}

func (l *readerWriterLatch) RUnlock() {
	// // TODO: (SDB) Asserts are for debug
	// SH_Assert(l.readerCount != 0, "Reader is not locked")
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.readerCount--

	if l.writerEntered {
		if l.readerCount == 0 {
			l.writer.Signal()
		}
	} else {
		if l.readerCount == MaxReaders-1 {
			l.reader.Signal()
		}
	}
}

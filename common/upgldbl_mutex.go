package common

import (
	"sync"
	"sync/atomic"
)

// UpgradableMutex is simple implementation of Upgradable RWMutex.
type UpgradableMutex struct {
	rwmu *sync.RWMutex
	u    int32
}

func NewUpgradableMutex() ReaderWriterLatch {
	mutex := &UpgradableMutex{}
	mutex.rwmu = new(sync.RWMutex)

	return mutex
}

// RLock locks shared for multi reader.
func (m *UpgradableMutex) RLock() {
	m.rwmu.RLock()
}

// RUnlock unlocks reader lock.
func (m *UpgradableMutex) RUnlock() {
	m.rwmu.RUnlock()
}

// WLock locks exclusively for single writer.
func (m *UpgradableMutex) WLock() {
lock:
	m.rwmu.Lock()
	if atomic.LoadInt32(&m.u) > 0 {
		// Upgrade is given priority to WLock, retry lock.
		m.rwmu.Unlock()
		goto lock
	}
}

// WUnlock unlocks writer lock.
func (m *UpgradableMutex) WUnlock() {
	m.rwmu.Unlock()
}

// Upgrade converts reader lock to writer lock and returns success (true) or dead-lock (false).
// If Upgrade by multi reader locker at same time then dead-lock.
// Upgrade is given priority to WLock.
func (m *UpgradableMutex) Upgrade() bool {
	success := atomic.AddInt32(&m.u, 1) == 1
	if success {
		m.rwmu.RUnlock()
		m.rwmu.Lock()
	}
	atomic.AddInt32(&m.u, -1)
	return success
}

func (m *UpgradableMutex) PrintDebugInfo() {
	//do nothing
}

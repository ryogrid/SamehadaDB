package common

import "sync"

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

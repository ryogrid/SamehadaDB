// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package types

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"unsafe"
)

// LSN is the type of the log identifier
type LSN int32

const SizeOfLSN = 4

// Serialize casts it to []byte
func (lsn LSN) Serialize() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, lsn)
	return buf.Bytes()
}

// NewLSNFromBytes creates a LSN from []byte
func NewLSNFromBytes(data []byte) (ret LSN) {
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &ret)
	return ret
}

func (lsn LSN) AtomicAdd(val int32) {
	p := (*int32)(unsafe.Pointer(&lsn))
	atomic.AddInt32(p, val)
}

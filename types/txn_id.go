// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package types

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"unsafe"
)

// TxnID is the type of the transaction identifier
type TxnID int32

// Serialize casts it to []byte
func (id TxnID) Serialize() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, id)
	return buf.Bytes()
}

// NewPageIDFromBytes creates a page id from []byte
func NewTxnIDFromBytes(data []byte) (ret TxnID) {
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &ret)
	return ret
}

func (id TxnID) AtomicAdd(val int32) {
	p := (*int32)(unsafe.Pointer(&id))
	atomic.AddInt32(p, val)
}

package types

import (
	"bytes"
	"encoding/binary"
)

type UInt32 uint32

// Serialize casts it to []byte
func (id UInt32) Serialize() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, id)
	return buf.Bytes()
}

// NewUInt32FromBytes creates a page id from []byte
func NewUInt32FromBytes(data []byte) (ret UInt32) {
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &ret)
	return ret
}

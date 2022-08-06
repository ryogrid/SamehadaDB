// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package types

import (
	"bytes"
	"encoding/binary"
)

type UInt32 uint32
type Int32 int32
type Bool bool

// Serialize casts it to []byte
func (id UInt32) Serialize() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, id)
	return buf.Bytes()
}

func NewUInt32FromBytes(data []byte) (ret UInt32) {
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &ret)
	return ret
}

// Serialize casts it to []byte
func (id Int32) Serialize() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, id)
	return buf.Bytes()
}

func NewInt32FromBytes(data []byte) (ret Int32) {
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &ret)
	return ret
}

// Serialize casts it to []byte
func (flag Bool) Serialize() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, flag)
	return buf.Bytes()
}

func NewBoolFromBytes(data []byte) (ret Bool) {
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &ret)
	return ret
}

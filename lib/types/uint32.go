// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package types

import (
	"bytes"
	"encoding/binary"
)

type UInt16 uint16
type UInt32 uint32
type UInt64 uint64
type Int32 int32
type Bool bool

// Serialize casts it to []byte
func (id UInt16) Serialize() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, id)
	return buf.Bytes()
}

func NewUInt16FromBytes(data []byte) (ret_ UInt16) {
	var ret UInt16
	binary.Read(bytes.NewBuffer(data), binary.BigEndian, &ret)
	return ret
}

// Serialize casts it to []byte
func (id UInt32) Serialize() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, id)
	return buf.Bytes()
}

// Serialize casts it to []byte
func (id UInt64) Serialize() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, id)
	return buf.Bytes()
}

func NewUInt32FromBytes(data []byte) (ret_ UInt32) {
	var ret UInt32
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &ret)
	return ret
}

func NewUInt64FromBytes(data []byte) (ret_ UInt64) {
	var ret UInt64
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &ret)
	return ret
}

// Serialize casts it to []byte
func (id Int32) Serialize() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, id)
	return buf.Bytes()
}

func NewInt32FromBytes(data []byte) (ret_ Int32) {
	var ret Int32
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &ret)
	return ret
}

// Serialize casts it to []byte
func (flag Bool) Serialize() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, flag)
	return buf.Bytes()
}

func NewBoolFromBytes(data []byte) (ret_ Bool) {
	var ret Bool
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &ret)
	return ret
}

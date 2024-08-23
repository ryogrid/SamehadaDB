package index_common

import (
	"bytes"
	"encoding/binary"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

const sizeEntryValue = uint32(8)

type IndexEntry struct {
	Key   types.Value
	Value uint64
}

func (ie IndexEntry) Serialize() []byte {
	keyInBytes := ie.Key.Serialize()
	valBuf := new(bytes.Buffer)
	binary.Write(valBuf, binary.LittleEndian, ie.Value)
	valInBytes := valBuf.Bytes()

	retBuf := new(bytes.Buffer)
	retBuf.Write(keyInBytes)
	retBuf.Write(valInBytes)
	return retBuf.Bytes()
}

func (ie IndexEntry) GetDataSize() uint32 {
	keyInBytes := ie.Key.Serialize()

	return uint32(len(keyInBytes)) + sizeEntryValue
}

func NewIndexEntryFromBytes(buf []byte, keyType types.TypeID) *IndexEntry {
	dataLen := len(buf)
	valPartOffset := dataLen - int(sizeEntryValue)
	key := types.NewValueFromBytes(buf[:valPartOffset], keyType)
	value := uint64(types.NewUInt64FromBytes(buf[valPartOffset:]))
	return &IndexEntry{*key, value}
}

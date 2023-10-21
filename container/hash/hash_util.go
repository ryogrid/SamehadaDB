package hash

import (
	"encoding/binary"
	"fmt"

	"github.com/ryogrid/SamehadaDB/types"
	"github.com/spaolacci/murmur3"
)

/** @return the hash of the value */
func HashValue(val *types.Value) uint32 {
	switch val.ValueType() {
	case types.Integer:
		raw := val.Serialize()
		return GenHashMurMur(raw)
	case types.Float:
		raw := val.Serialize()
		return GenHashMurMur(raw)
	case types.Varchar:
		raw := val.Serialize()
		return GenHashMurMur(raw)
	default:
		fmt.Println(val.ValueType())
		panic("not supported type!")
	}
}

func GenHashMurMur(key []byte) uint32 {
	h := murmur3.New128()
	h.Write(key)

	hash := h.Sum(nil)

	return binary.LittleEndian.Uint32(hash)
}

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
	// case types.TINYINT:
	// 	raw := static_cast<int64_t>(val.GetAs<int8_t>())
	// 	return Hash<int64_t>(&raw)
	// case types.SMALLINT:
	// 	raw := static_cast<int64_t>(val.GetAs<int16_t>())
	// 	return Hash<int64_t>(&raw)
	case types.Integer:
		//raw := static_cast<int64_t>(val.GetAs<int32_t>())
		raw := val.Serialize()
		return GenHashMurMur(raw)
	case types.Float:
		raw := val.Serialize()
		return GenHashMurMur(raw)
	// case types.BIGINT:
	// 	raw := static_cast<int64_t>(val.GetAs<int64_t>())
	// 	return Hash<int64_t>(&raw)
	// case types.BOOLEAN:
	// 	raw := val.GetAs<bool>()
	// 	return Hash<bool>(&raw)
	// case types.DECIMAL:
	// 	raw := val.GetAs<double>()
	// 	return Hash<double>(&raw)
	case types.Varchar:
		raw := val.Serialize()
		return GenHashMurMur(raw)
	// case types.TIMESTAMP:
	// 	raw := val.GetAs<uint64_t>()
	// 	return Hash<uint64_t>(&raw)
	default:
		fmt.Println(val.ValueType())
		panic("not supported type!")
		//BUSTUB_ASSERT(false, "Unsupported type.")
	}
}

func GenHashMurMur(key []byte) uint32 {
	h := murmur3.New128()
	h.Write(key)

	hash := h.Sum(nil)

	return binary.LittleEndian.Uint32(hash)
}

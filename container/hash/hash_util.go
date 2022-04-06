package hash

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ryogrid/SamehadaDB/types"
	"github.com/spaolacci/murmur3"
)

const prime_factor uint32 = 10000019

func hashBytes(bytes []byte, length uint32) uint32 {
	// https://github.com/greenplum-db/gpos/blob/b53c1acd6285de94044ff91fbee91589543feba1/libgpos/src/utils.cpp#L126
	var hash uint32 = length
	for i := 0; i < int(length); i++ {
		hash = ((hash << 5) ^ (hash >> 27)) ^ uint32(bytes[i])
	}
	return hash
}

func CombineHashes(l uint32, r uint32) uint32 {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, l)
	binary.Write(buf, binary.LittleEndian, r)
	return hashBytes(buf.Bytes(), 4*2)
}

func SumHashes(l uint32, r uint32) uint32 { return (l%prime_factor + r%prime_factor) % prime_factor }

// func Hash(ptr *T) uint32 {
// 	return HashBytes(ptr, sizeof(T))
// }

// func  HashPtr( *ptr *T) uint32 {
// 	return HashBytes(reinterpret_cast< char *>(&ptr), sizeof(void *))
// }

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
	//bs := make([]byte, 4)
	//binary.LittleEndian.PutUint32(bs, uint32(key))

	//h.Write(bs)
	h.Write(key)

	hash := h.Sum(nil)

	//return int(binary.LittleEndian.Uint32(hash))
	return binary.LittleEndian.Uint32(hash)
}

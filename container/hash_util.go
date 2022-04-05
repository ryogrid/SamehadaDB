package hash

import (
	"bytes"
	"encoding/binary"
)

const prime_factor uint32 = 10000019

func HashBytes(bytes []byte, length uint32) uint32 {
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
	//return HashBytes(both, sizeof(uint32)*2)
	return HashBytes(buf.Bytes(), 4*2)
}

func SumHashes(l uint32, r uint32) uint32 { return (l%prime_factor + r%prime_factor) % prime_factor }

// func Hash(ptr *T) uint32 {
// 	return HashBytes(ptr, sizeof(T))
// }

// func  HashPtr( *ptr *T) uint32 {
// 	return HashBytes(reinterpret_cast< char *>(&ptr), sizeof(void *))
// }

// /** @return the hash of the value */
// func  HashValue(  val  *values.Value) uint32 {
// 	switch (val.GetTypeId()) {
// 		// case types.TINYINT:
// 		// 	raw := static_cast<int64_t>(val.GetAs<int8_t>())
// 		// 	return Hash<int64_t>(&raw)
// 		// case types.SMALLINT:
// 		// 	raw := static_cast<int64_t>(val.GetAs<int16_t>())
// 		// 	return Hash<int64_t>(&raw)
// 		case types.INTEGER:
// 			raw := static_cast<int64_t>(val.GetAs<int32_t>())
// 			return Hash<int64_t>(&raw)
// 		// case types.BIGINT:
// 		// 	raw := static_cast<int64_t>(val.GetAs<int64_t>())
// 		// 	return Hash<int64_t>(&raw)
// 		// case types.BOOLEAN:
// 		// 	raw := val.GetAs<bool>()
// 		// 	return Hash<bool>(&raw)
// 		// case types.DECIMAL:
// 		// 	raw := val.GetAs<double>()
// 		// 	return Hash<double>(&raw)
// 		case types.Varchar:
// 			raw := val.GetData()
// 			len = val.GetLength()
// 			return HashBytes(raw, len)
// 		// case types.TIMESTAMP:
// 		// 	raw := val.GetAs<uint64_t>()
// 		// 	return Hash<uint64_t>(&raw)
// 		default:
// 		//BUSTUB_ASSERT(false, "Unsupported type.")
// 	}
// }

package index

import (
	"bytes"
	"encoding/binary"

	hash "github.com/ryogrid/SamehadaDB/container/hash"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

type LinearProbeHashTableIndex struct {
	// comparator for key
	//KeyComparator comparator_;
	// container
	container hash.LinearProbeHashTable
	metadata  *IndexMetadata
	// idx of target column on table
	col_idx uint32
}

func NewLinearProbeHashTableIndex(metadata *IndexMetadata, buffer_pool_manager *buffer.BufferPoolManager, col_idx uint32,
	num_buckets int) *LinearProbeHashTableIndex {
	ret := new(LinearProbeHashTableIndex)
	ret.metadata = metadata
	ret.container = *hash.NewLinearProbeHashTable(buffer_pool_manager, num_buckets)
	ret.col_idx = col_idx
	return ret
}

// Return the metadata object associated with the index
func (htidx *LinearProbeHashTableIndex) GetMetadata() *IndexMetadata { return htidx.metadata }

func (htidx *LinearProbeHashTableIndex) GetIndexColumnCount() uint32 {
	return htidx.metadata.GetIndexColumnCount()
}
func (htidx *LinearProbeHashTableIndex) GetName() *string { return htidx.metadata.GetName() }
func (htidx *LinearProbeHashTableIndex) GetTupleSchema() *schema.Schema {
	return htidx.metadata.GetTupleSchema()
}
func (htidx *LinearProbeHashTableIndex) GetKeyAttrs() []uint32 { return htidx.metadata.GetKeyAttrs() }

func (htidx *LinearProbeHashTableIndex) InsertEntry(key *tuple.Tuple, rid page.RID, transaction *access.Transaction) {
	tupleSchema_ := htidx.GetTupleSchema()
	keyDataInBytes := key.GetValueInBytes(tupleSchema_, htidx.col_idx)

	htidx.container.Insert(keyDataInBytes, PackRIDtoUint32(&rid))
}

// TODO: (SDB) not tested yet (DeleteEntry at LinearProbeHashTableIndex)
// TODO: (SDB) need test DeleteEntry of LinearProbeHashTableIndex deletes appropriately key duplicated case
//             (all entry correspoind to a key must not be deleted)
func (htidx *LinearProbeHashTableIndex) DeleteEntry(key *tuple.Tuple, rid page.RID, transaction *access.Transaction) {
	tupleSchema_ := htidx.GetTupleSchema()
	keyDataInBytes := key.GetValueInBytes(tupleSchema_, htidx.col_idx)

	htidx.container.Remove(keyDataInBytes, PackRIDtoUint32(&rid))
}

func (htidx *LinearProbeHashTableIndex) ScanKey(key *tuple.Tuple, transaction *access.Transaction) []page.RID {
	tupleSchema_ := htidx.GetTupleSchema()
	keyDataInBytes := key.GetValueInBytes(tupleSchema_, htidx.col_idx)

	packed_values := htidx.container.GetValue(keyDataInBytes)
	var ret_arr []page.RID
	for _, packed_val := range packed_values {
		ret_arr = append(ret_arr, UnpackUint32toRID(packed_val))
	}
	return ret_arr
}

func PackRIDtoUint32(value *page.RID) uint32 {
	buf1 := new(bytes.Buffer)
	buf2 := new(bytes.Buffer)
	pack_buf := make([]byte, 4)
	binary.Write(buf1, binary.LittleEndian, value.PageId)
	binary.Write(buf2, binary.LittleEndian, value.SlotNum)
	pageIdInBytes := buf1.Bytes()
	slotNumInBytes := buf2.Bytes()
	copy(pack_buf[:2], pageIdInBytes[:2])
	copy(pack_buf[2:], slotNumInBytes[:2])
	return binary.LittleEndian.Uint32(pack_buf)
}

func UnpackUint32toRID(value uint32) page.RID {
	packed_buf := new(bytes.Buffer)
	binary.Write(packed_buf, binary.LittleEndian, value)
	packedDataInBytes := packed_buf.Bytes()
	var PageId types.PageID
	var SlotNum uint32
	buf := make([]byte, 4)
	copy(buf[:2], packedDataInBytes[:2])
	PageId = types.PageID(binary.LittleEndian.Uint32(buf))
	copy(buf[:2], packedDataInBytes[2:])
	SlotNum = binary.LittleEndian.Uint32(buf)
	ret := new(page.RID)
	ret.PageId = PageId
	ret.SlotNum = SlotNum
	return *ret
}

package index

// TODO: (SDB) need port LinearProbeHashTableIndex class
/*
#define HASH_TABLE_INDEX_TYPE LinearProbeHashTableIndex<KeyType, ValueType, KeyComparator>

template <typename KeyType, typename ValueType, typename KeyComparator>
class LinearProbeHashTableIndex : public Index {

	// Constructor
	template <typename KeyType, typename ValueType, typename KeyComparator>
	HASH_TABLE_INDEX_TYPE::LinearProbeHashTableIndex(IndexMetadata *metadata, BufferPoolManager *buffer_pool_manager,
													size_t num_buckets, const HashFunction<KeyType> &hash_fn)
		: Index(metadata),
		comparator_(metadata->GetKeySchema()),
		container_(metadata->GetName(), buffer_pool_manager, comparator_, num_buckets, hash_fn) {}

	// Return the metadata object associated with the index
	IndexMetadata *GetMetadata() const { return metadata_; }

	int GetIndexColumnCount() const { return metadata_->GetIndexColumnCount(); }

	const std::string &GetName() const { return metadata_->GetName(); }

	Schema *GetKeySchema() const { return metadata_->GetKeySchema(); }

	const std::vector<uint32_t> &GetKeyAttrs() const { return metadata_->GetKeyAttrs(); }

	template <typename KeyType, typename ValueType, typename KeyComparator>
	void HASH_TABLE_INDEX_TYPE::InsertEntry(const Tuple &key, RID rid, Transaction *transaction) {
	// construct insert index key
	KeyType index_key;
	index_key.SetFromKey(key);

	container_.Insert(transaction, index_key, rid);
	}

	template <typename KeyType, typename ValueType, typename KeyComparator>
	void HASH_TABLE_INDEX_TYPE::DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) {
	// construct delete index key
	KeyType index_key;
	index_key.SetFromKey(key);

	container_.Remove(transaction, index_key, rid);
	}

	template <typename KeyType, typename ValueType, typename KeyComparator>
	void HASH_TABLE_INDEX_TYPE::ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) {
	// construct scan index key
	KeyType index_key;
	index_key.SetFromKey(key);

	container_.GetValue(transaction, index_key, result);
	}

 protected:
	// comparator for key
	KeyComparator comparator_;
	// container
	LinearProbeHashTable<KeyType, ValueType, KeyComparator> container_;
   	IndexMetadata *metadata_;
};
*/

package index_constants

type IndexKind int32

const (
	IndexKindInvalid IndexKind = iota
	IndexKindUniqSkipList
	IndexKindSkipList
	IndexKindHash
	IndexKindBtree // B-link tree
)

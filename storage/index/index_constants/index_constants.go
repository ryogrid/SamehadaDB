package index_constants

type IndexKind int32

const (
	INDEX_KIND_INVALID IndexKind = iota
	INDEX_KIND_UNIQ_SKIP_LIST
	INDEX_KIND_SKIP_LIST
	INDEX_KIND_HASH
)

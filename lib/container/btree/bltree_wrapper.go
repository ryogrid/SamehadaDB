package btree

import (
	"github.com/ryogrid/SamehadaDB/lib/types"
	blink_tree "github.com/ryogrid/bltree-go-for-embedding"
)

type BLTreeWrapper struct {
	*blink_tree.BLTree
	bufMgr *blink_tree.BufMgr
}

func NewBLTreeWrapper(bltree *blink_tree.BLTree, bufMgr *blink_tree.BufMgr) *BLTreeWrapper {
	return &BLTreeWrapper{bltree, bufMgr}
}

func (bltw *BLTreeWrapper) GetValue(key *types.Value) uint64 {
	// TODO: (SDB) need to implement this
	panic("Not implemented yet")
}

func (bltw *BLTreeWrapper) Insert(key *types.Value, value uint64) (err error) {
	// TODO: (SDB) need to implement this
	panic("Not implemented yet")
}

func (bltw *BLTreeWrapper) Remove(key *types.Value, value uint64) (isDeleted_ bool) {
	// TODO: (SDB) need to implement this
	panic("Not implemented yet")
}

func (bltw *BLTreeWrapper) Iterator(rangeStartKey *types.Value, rangeEndKey *types.Value) *BTreeIterator {
	// TODO: (SDB) need to implement this
	panic("Not implemented yet")
}

// call this at shutdown of the system
// to write out the state and allocated pages of the container to BPM
func (bltw *BLTreeWrapper) WriteOutContainerStateToBPM() {
	bltw.bufMgr.Close()
}

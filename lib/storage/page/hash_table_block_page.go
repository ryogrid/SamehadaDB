// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package page

import "github.com/ryogrid/SamehadaDB/lib/common"

type HashTablePair struct {
	key   uint64 //uint32
	value uint64
}

const sizeOfHashTablePair = 16 //12
const BlockArraySize = 4 * common.PageSize / (4*sizeOfHashTablePair + 1)

/**
 * Store indexed key and value together within block page. Supports
 * non-unique keys.
 *
 * Block page format (keys are stored in order):
 *  ----------------------------------------------------------------
 * | KEY(1) + VALUE(1) | KEY(2) + VALUE(2) | ... | KEY(n) + VALUE(n)
 *  ----------------------------------------------------------------
 *
 *  Here '+' means concatenation.
 *
 */
type HashTableBlockPage struct {
	occuppied [(BlockArraySize-1)/8 + 1]byte // 43 bytes (344 bits)
	readable  [(BlockArraySize-1)/8 + 1]byte // 43 bytes (344 bits)
	array     [BlockArraySize]HashTablePair  // 334 * 12 bytes
}

// Gets the key at an index in the block
func (page *HashTableBlockPage) KeyAt(index uint64) uint64 {
	return page.array[index].key
}

// Gets the value at an index in the block
func (page *HashTableBlockPage) ValueAt(index uint64) uint64 {
	return page.array[index].value
}

// Attempts to insert a key and value into an index in the baccess.
func (page *HashTableBlockPage) Insert(index uint64, key uint64, value uint64) bool {
	if page.IsOccupied(index) && page.IsReadable(index) {
		return false
	}

	page.array[index] = HashTablePair{key, value}
	page.occuppied[index/8] |= 1 << (index % 8)
	page.readable[index/8] |= 1 << (index % 8)
	return true
}

func (page *HashTableBlockPage) Remove(index uint64) {
	if !page.IsReadable(index) {
		return
	}

	page.readable[index/8] &= ^(1 << (index % 8))
}

// Returns whether or not an index is occuppied (valid key/value pair)
func (page *HashTableBlockPage) IsOccupied(index uint64) bool {
	return (page.occuppied[index/8] & (1 << (index % 8))) != 0
}

// Returns whether or not an index is readable (valid key/value pair)
func (page *HashTableBlockPage) IsReadable(index uint64) bool {
	return (page.readable[index/8] & (1 << (index % 8))) != 0
}

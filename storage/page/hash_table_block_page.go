// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in license/go-bustub dir

package page

type HashTablePair struct {
	key   int
	value int
}

const sizeOfHashTablePair = 16
const BlockArraySize = 4 * 4096 / (4*sizeOfHashTablePair + 1)

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
	occuppied [(BlockArraySize-1)/8 + 1]byte // 256 bits
	readable  [(BlockArraySize-1)/8 + 1]byte // 256 bits
	array     [BlockArraySize]HashTablePair  // 252 * 16 bits
}

// Gets the key at an index in the block
func (page *HashTableBlockPage) KeyAt(index int) int {
	return page.array[index].key
}

// Gets the value at an index in the block
func (page *HashTableBlockPage) ValueAt(index int) int {
	return page.array[index].value
}

// Attempts to insert a key and value into an index in the block.
func (page *HashTableBlockPage) Insert(index int, key int, value int) bool {
	if page.IsOccupied(index) {
		return false
	}

	page.array[index] = HashTablePair{key, value}
	page.occuppied[index/8] |= (1 << (index % 8))
	page.readable[index/8] |= (1 << (index % 8))
	return true
}

func (page *HashTableBlockPage) Remove(index int) {
	if !page.IsReadable(index) {
		return
	}

	page.readable[index/8] &= ^(1 << (index % 8))
}

// Returns whether or not an index is occuppied (valid key/value pair)
func (page *HashTableBlockPage) IsOccupied(index int) bool {
	return (page.occuppied[index/8] & (1 << (index % 8))) != 0
}

// Returns whether or not an index is readable (valid key/value pair)
func (page *HashTableBlockPage) IsReadable(index int) bool {
	return (page.readable[index/8] & (1 << (index % 8))) != 0
}

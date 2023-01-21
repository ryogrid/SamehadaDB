// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package types

import (
	"bytes"
	"encoding/binary"
	"github.com/ryogrid/SamehadaDB/errors"
)

// PageID is the type of the page identifier
type PageID int32

const DeallocatedPageErr = errors.Error("dellocated Page ID is passed.")

// InvalidPageID represents an invalid page GetPageId
const InvalidPageID = PageID(-1)

// IsValid checks if id is valid
func (id PageID) IsValid() bool {
	return id != InvalidPageID || id >= 0
}

// Serialize casts it to []byte
func (id PageID) Serialize() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, id)
	return buf.Bytes()
}

// NewPageIDFromBytes creates a page id from []byte
func NewPageIDFromBytes(data []byte) (ret PageID) {
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &ret)
	return ret
}

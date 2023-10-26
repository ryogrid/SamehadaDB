// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package types

type TypeID int

// Every possible SQL type GetPageId
const (
	Invalid TypeID = iota
	Boolean
	Tinyint
	Smallint
	Integer
	BigInt
	Decimal
	Float
	Varchar
	Timestamp
	Null
)

func (t TypeID) Size() uint32 {
	switch t {
	case Integer:
		return 1 + 4
	case Float:
		return 1 + 4
	case Boolean:
		return 1 + 1
	}
	return 0
}

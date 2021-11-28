// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in license/go-bustub dir

package types

type TypeID int

const (
	Invalid TypeID = iota
	Boolean
	Tinyint
	Smallint
	Integer
	BigInt
	Decimal
	Varchar
	Timestamp
)

func (t TypeID) Size() uint32 {
	switch t {
	case Integer:
		return 4
	}
	return 0
}

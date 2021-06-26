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

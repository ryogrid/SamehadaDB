package types

type IntegerType struct {
	value int32
}

func NewIntegerType(value int32) IntegerType {
	return IntegerType{value}
}

func (i IntegerType) CompareEquals(v IntegerType) bool {
	return i.value == v.value
}

func (i IntegerType) CompareNotEquals(v IntegerType) bool {
	return i.value != v.value
}

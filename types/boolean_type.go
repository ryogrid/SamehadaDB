package types

type BooleanType struct {
	value bool
}

func NewBooleanType(value bool) BooleanType {
	return BooleanType{value}
}

func (t BooleanType) IsTrue() bool {
	return t.value
}

// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package testing_util

import (
	"github.com/ryogrid/SamehadaDB/types"
)

func GetValue(data interface{}) (value types.Value) {
	switch v := data.(type) {
	case int:
		value = types.NewInteger(int32(v))
	case int32:
		value = types.NewInteger(v)
	case float32:
		value = types.NewFloat(float32(v))
	case string:
		value = types.NewVarchar(v)
	case bool:
		value = types.NewBoolean(v)
	case *types.Value:
		val := data.(*types.Value)
		return *val
	}
	return
}

func GetValueType(data interface{}) (value types.TypeID) {
	switch data.(type) {
	case int, int32:
		return types.Integer
	case float32:
		return types.Float
	case string:
		return types.Varchar
	case bool:
		return types.Boolean
	case *types.Value:
		val := data.(*types.Value)
		return val.ValueType()
	}
	panic("not implemented")
}

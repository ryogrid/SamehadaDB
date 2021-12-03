// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package table

import (
	"testing"

	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
)

func TestTuple(t *testing.T) {
	columnA := NewColumn("a", types.Integer)
	columnB := NewColumn("b", types.Varchar)
	columnC := NewColumn("c", types.Integer)
	columnD := NewColumn("d", types.Varchar)
	columnE := NewColumn("e", types.Varchar)

	schema := NewSchema([]*Column{columnA, columnB, columnC, columnD, columnE})

	row := make([]types.Value, 0)

	expA, expB, expC, expD, expE := int32(99), "Hello World", int32(100), "áé&@#+\\çç", "blablablablabalbalalabalbalbalablablabalbalaba"
	row = append(row, types.NewInteger(expA))
	row = append(row, types.NewVarchar(expB))
	row = append(row, types.NewInteger(expC))
	row = append(row, types.NewVarchar(expD))
	row = append(row, types.NewVarchar(expE))
	tuple := NewTupleFromSchema(row, schema)

	testingpkg.Equals(t, expA, tuple.GetValue(schema, 0).ToInteger())
	testingpkg.Equals(t, expB, tuple.GetValue(schema, 1).ToVarchar())
	testingpkg.Equals(t, expC, tuple.GetValue(schema, 2).ToInteger())
	testingpkg.Equals(t, expD, tuple.GetValue(schema, 3).ToVarchar())
	testingpkg.Equals(t, expE, tuple.GetValue(schema, 4).ToVarchar())

	testingpkg.Equals(t, uint32(96), tuple.Size())
}

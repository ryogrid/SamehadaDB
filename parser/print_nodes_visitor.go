package parser

import (
	"fmt"
	"github.com/pingcap/parser/ast"
	"reflect"
)

type PrintNodesVisitor struct {
}

func NewPrintNodesVisitor() *PrintNodesVisitor {
	return new(PrintNodesVisitor)
}

func (v *PrintNodesVisitor) Enter(in ast.Node) (ast.Node, bool) {
	refVal := reflect.ValueOf(in)
	fmt.Println(refVal.Type())
	return in, false
}

func (v *PrintNodesVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

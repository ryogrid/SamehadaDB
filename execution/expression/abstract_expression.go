package expression

import "github.com/ryogrid/SamehadaDB/types"

type AbstractExpression struct {
	/** The children of this expression. Note that the order of appearance of children may matter. */
	children [2]Expression
	/** The return type of this expression. */
	ret_type types.TypeID
}

// /** @return the child_idx'th child of this expression */
// func (e *AbstractExpression) GetChildAt(child_idx uint32) *Expression { return e.children[child_idx] }

// /** @return the children of this expression, ordering may matter */
// func (e *AbstractExpression) GetChildren() []*Expression { return e.children }

// /** @return the type of this expression if it were to be evaluated */
// func (e *AbstractExpression) GetReturnType() types.TypeID { return e.ret_type }

package expression

import "github.com/ryogrid/SamehadaDB/types"

type AbstractExpression struct {
	/** The children of this expression. Note that the order of appearance of children may matter. */
	children [2]Expression
	/** The return type of this expression. */
	ret_type types.TypeID
}

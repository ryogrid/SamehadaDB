package expression

import "math"

// TODO: (SDB) [OPT] not implemented yet (PrintExpTree at expression_util.go)
func PrintExpTree(exp Expression, inputStr string) string {
	retStr := inputStr

	childTraverse := func(exp Expression) string {
		var idx uint32 = 0
		var tmpStr string = ""
		for exp.GetChildAt(idx) != nil && idx < math.MaxInt32 {
			child := exp.GetChildAt(idx)
			tmpStr += PrintExpTree(child, retStr)
			idx++
		}
		return tmpStr
	}

	switch exp.(type) {
	case *Comparison:
		return retStr + childTraverse(exp)
	case *ConstantValue:
		return retStr + childTraverse(exp)
	case *AggregateValueExpression:
		return retStr + childTraverse(exp)
	case *ColumnValue:
		return retStr + childTraverse(exp)
	default:
		panic("illegal type expression object is passed!")
	}
	//print(exp.GetDebugStr())
	//println()

}

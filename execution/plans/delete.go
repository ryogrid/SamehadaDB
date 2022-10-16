package plans

/**
 * DeletePlanNode identifies a table and conditions specify record to be deleted.
 */
type DeletePlanNode struct {
	*AbstractPlanNode
	//predicate expression.Expression
	//tableOID uint32
}

// func NewDeletePlanNode(predicate expression.Expression, oid uint32) Plan {
func NewDeletePlanNode(child Plan) Plan {
	return &DeletePlanNode{&AbstractPlanNode{nil, []Plan{child}}}
}

func (p *DeletePlanNode) GetTableOID() uint32 {
	//return p.tableOID
	return p.children[0].GetTableOID()
}

//func (p *DeletePlanNode) GetPredicate() expression.Expression {
//	return p.predicate
//}

func (p *DeletePlanNode) GetType() PlanType {
	return Delete
}

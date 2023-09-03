package plans

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * UpdatePlanNode identifies a table and conditions specify record to be deleted.
 */
type UpdatePlanNode struct {
	*AbstractPlanNode
	rawValues       []types.Value
	update_col_idxs []int
	stats_          *catalog.TableStatistics
	//predicate       expression.Expression
	//tableOID        uint32
}

// if you update all column, you can specify nil to update_col_idxs. then all data of existed tuple is replaced with rawValues
// if you want update specifed columns only, you should specify columns with update_col_idxs and pass rawValues of all columns defined in schema.
// but not update target column value can be dummy value!
// func NewUpdatePlanNode(rawValues []types.Value, update_col_idxs []int, predicate expression.Expression, oid uint32) Plan {
func NewUpdatePlanNode(rawValues []types.Value, update_col_idxs []int, child Plan) Plan {
	return &UpdatePlanNode{&AbstractPlanNode{nil, []Plan{child}}, rawValues, update_col_idxs, child.GetStatistics().GetDeepCopy()}
}

func (p *UpdatePlanNode) GetTableOID() uint32 {
	return p.children[0].GetTableOID()
}

//func (p *UpdatePlanNode) GetPredicate() expression.Expression {
//	return p.predicate
//}

func (p *UpdatePlanNode) GetType() PlanType {
	return Delete
}

// GetRawValues returns the raw values to be overwritten data
func (p *UpdatePlanNode) GetRawValues() []types.Value {
	return p.rawValues
}

func (p *UpdatePlanNode) GetUpdateColIdxs() []int {
	return p.update_col_idxs
}

func (p *UpdatePlanNode) AccessRowCount(c *catalog.Catalog) uint64 {
	return p.children[0].AccessRowCount(c)
}

func (p *UpdatePlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	return p.children[0].EmitRowCount(c)
}

func (p *UpdatePlanNode) GetTreeInfoStr() string {
	// TODO: (SDB) [OPT] not implemented yet (UpdatePlanNode::GetTreeInfoStr)
	panic("not implemented yet")
}

func (p *UpdatePlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats_
}

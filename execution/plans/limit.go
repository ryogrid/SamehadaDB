// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

type LimitPlanNode struct {
	*AbstractPlanNode
	limit  uint32
	offset uint32
}

func NewLimitPlanNode(child Plan, limit uint32, offset uint32) Plan {
	return &LimitPlanNode{&AbstractPlanNode{nil, []Plan{child}}, limit, offset}
}

func (p *LimitPlanNode) GetLimit() uint32 {
	return p.limit
}

func (p *LimitPlanNode) GetOffset() uint32 {
	return p.offset
}

func (p *LimitPlanNode) GetType() PlanType {
	return Limit
}

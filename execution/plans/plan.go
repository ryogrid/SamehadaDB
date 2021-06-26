package plans

import "github.com/brunocalza/go-bustub/storage/table"

type PlanType int

const (
	SeqScan PlanType = iota
	Insert
)

type Plan interface {
	OutputSchema() *table.Schema
	GetChildAt(childIndex uint32) Plan
	GetChildren() []Plan
	GetType() PlanType
}

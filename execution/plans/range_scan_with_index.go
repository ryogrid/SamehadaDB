// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"strconv"
)

/**
 * RangeScanWithIndexPlanNode use hash index to filter rows matches predicate.
 */
type RangeScanWithIndexPlanNode struct {
	*AbstractPlanNode
	predicate  expression.Expression
	tableOID   uint32
	colIdx     int32 // column idx which has index to be used
	startRange *types.Value
	endRange   *types.Value
	stats_     *catalog.TableStatistics
}

func NewRangeScanWithIndexPlanNode(c *catalog.Catalog, schema *schema.Schema, tableOID uint32, colIdx int32, predicate expression.Expression, startRange *types.Value, endRange *types.Value) Plan {
	tm := c.GetTableByOID(tableOID)
	//return &RangeScanWithIndexPlanNode{&AbstractPlanNode{schema, nil}, predicate, tableOID, colIdx, startRange, endRange, tm.GetStatistics().GetDeepCopy()}
	ret := &RangeScanWithIndexPlanNode{&AbstractPlanNode{schema, nil}, predicate, tableOID, colIdx, startRange, endRange, tm.GetStatistics().GetDeepCopy()}
	ret.stats_ = ret.stats_.TransformBy(colIdx, startRange, endRange)
	return ret
}

func (p *RangeScanWithIndexPlanNode) GetPredicate() expression.Expression {
	return p.predicate
}

func (p *RangeScanWithIndexPlanNode) GetTableOID() uint32 {
	return p.tableOID
}

func (p *RangeScanWithIndexPlanNode) GetColIdx() int32 {
	return p.colIdx
}

func (p *RangeScanWithIndexPlanNode) GetStartRange() *types.Value {
	return p.startRange
}

func (p *RangeScanWithIndexPlanNode) GetEndRange() *types.Value {
	return p.endRange
}

func (p *RangeScanWithIndexPlanNode) GetType() PlanType {
	return IndexRangeScan
}

func (p *RangeScanWithIndexPlanNode) AccessRowCount(c *catalog.Catalog) uint64 {
	return p.EmitRowCount(c)
}

func (p *RangeScanWithIndexPlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	return uint64(math.Ceil(c.GetTableByOID(p.tableOID).GetStatistics().EstimateCount(p.colIdx, p.startRange, p.endRange)))
}

func (p *RangeScanWithIndexPlanNode) GetDebugStr() string {
	outColNames := "["
	for _, col := range p.OutputSchema().GetColumns() {
		outColNames += col.GetColumnName() + ", "
	}
	return "RangeScanWithIndexPlanNode " + outColNames + "] " + "type:" + strconv.Itoa(int(p.startRange.ValueType())) + " start:" + p.startRange.ToString() + " end:" + p.endRange.ToString()
}

func (p *RangeScanWithIndexPlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats_
}

package executors

import (
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/container/hash"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/materialization"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

/**
* HashJoinExecutor executes hash join operations (inner join).
 */
type HashJoinExecutor struct {
	context *ExecutorContext
	/** The hash join plan node. */
	plan *plans.HashJoinPlanNode
	/** The hash table that we are using. */
	jht *SimpleHashJoinHashTable
	/** The number of buckets in the hash table. */
	jhtNumBuckets uint32 //= 2
	left            Executor
	right           Executor
	leftExpr       expression.Expression
	rightExpr      expression.Expression
	tmpTuples      []materialization.TmpTuple
	index           int32
	outputExprs    []expression.Expression
	tmpPageIDs    []types.PageID
	rightTuple     tuple.Tuple
}

/**
* Creates a new hash join executor.
* @param execCtx the context that the hash join should be performed in
* @param plan the hash join plan node
* @param left the left child, used by convention to build the hash table
* @param right the right child, used by convention to probe the hash table
 */
func NewHashJoinExecutor(execCtx *ExecutorContext, plan *plans.HashJoinPlanNode, left Executor,
	right Executor) *HashJoinExecutor {
	ret := new(HashJoinExecutor)
	ret.plan = plan
	ret.context = execCtx
	ret.left = left
	ret.right = right
	// about 200k entry can be stored
	ret.jhtNumBuckets = 100
	ret.jht = NewSimpleHashJoinHashTable()
	return ret
}

func (e *HashJoinExecutor) GetJHT() *SimpleHashJoinHashTable { return e.jht }

func (e *HashJoinExecutor) GetOutputSchema() *schema.Schema { return e.plan.OutputSchema() }

func (e *HashJoinExecutor) Init() {
	// get exprs to evaluate to output result
	outputColumnCnt := int(e.GetOutputSchema().GetColumnCount())
	for i := 0; i < outputColumnCnt; i++ {
		col := e.GetOutputSchema().GetColumn(uint32(i))
		var colVal expression.Expression
		if col.IsLeft() {
			colname := col.GetColumnName()
			colIndex := e.plan.GetLeftPlan().OutputSchema().GetColIndex(colname)
			colVal = expression.NewColumnValue(0, colIndex, types.Invalid)
		} else {
			colname := col.GetColumnName()
			colIndex := e.plan.GetRightPlan().OutputSchema().GetColIndex(colname)
			colVal = expression.NewColumnValue(1, colIndex, types.Invalid)
		}

		e.outputExprs = append(e.outputExprs, colVal)
	}
	// build hash table from left
	e.left.Init()
	e.right.Init()
	e.leftExpr = e.plan.OnPredicate().GetChildAt(0)
	e.rightExpr = e.plan.OnPredicate().GetChildAt(1)
	// store all the left tuples in tmp pages in that it can not fit in memory
	// use tmp tuple as the value of the hash table kv pair
	var tmpPage *materialization.TmpTuplePage = nil
	var tmpPageID types.PageID = common.InvalidPageID
	var tmpTuple materialization.TmpTuple
	for leftTuple, done, _ := e.left.Next(); !done; leftTuple, done, _ = e.left.Next() {
		if leftTuple == nil {
			return
		}
		if tmpPage == nil || !tmpPage.Insert(leftTuple, &tmpTuple) {
			// unpin the last full tmp page
			if tmpPageID != common.InvalidPageID {
				e.context.GetBufferPoolManager().UnpinPage(tmpPageID, true)
			}
			// create new tmp page
			page := e.context.GetBufferPoolManager().NewPage()
			tmpPage = materialization.CastPageAsTmpTuplePage(page)
			if tmpPage == nil {
				panic("fail to create new tmp page when doing hash join")
			}
			tmpPage.Init(tmpPage.GetPageID(), common.PageSize)
			tmpPageID = tmpPage.GetPageID()
			e.tmpPageIDs = append(e.tmpPageIDs, tmpPageID)
			// reinsert the tuple
			tmpPage.Insert(leftTuple, &tmpTuple)
		}
		valueAsKey := e.leftExpr.Evaluate(leftTuple, e.left.GetOutputSchema())
		if !valueAsKey.IsNull() {
			e.jht.Insert(hash.HashValue(&valueAsKey), &tmpTuple)
		}
	}
}

// TODO: (SDB) need to refactor HashJoinExecutor::Next method to use GetExpr method of Column class
//
//	current impl is avoiding the method because it does not exist when this code was wrote
func (e *HashJoinExecutor) Next() (*tuple.Tuple, Done, error) {
	innerNextCnt := 0
	for {
		for int(e.index) == len(e.tmpTuples) {
			// we have traversed all possible join combination of the current right tuple
			// move to the next right tuple
			e.tmpTuples = []materialization.TmpTuple{}
			e.index = 0
			var done Done = false
			var tmpTuple *tuple.Tuple
			if tmpTuple, done, _ = e.right.Next(); done {
				// hash join finished, delete all the tmp page we created
				for _, tmpPageID := range e.tmpPageIDs {
					e.context.GetBufferPoolManager().DeallocatePage(tmpPageID, true)
				}
				// tmpTuple should be nil
				return tmpTuple, true, nil
			}
			innerNextCnt++
			e.rightTuple = *tmpTuple
			value := e.rightExpr.Evaluate(&e.rightTuple, e.right.GetOutputSchema())
			if value.IsNull() {
				continue
			}
			e.tmpTuples = e.jht.GetValue(hash.HashValue(&value))
		}
		// traverse corresponding left tuples stored in the tmp pages until we find a tuple which satisfies the predicate with current right tuple
		leftTmpTuple := e.tmpTuples[e.index]
		var leftTuple tuple.Tuple
		materialization.FetchTupleFromTmpTuplePage(e.context.bpm, &leftTuple, &leftTmpTuple)
		for !e.IsValidCombination(&leftTuple, &e.rightTuple) {
			e.index++
			if int(e.index) == len(e.tmpTuples) {
				break
			}
			leftTmpTuple = e.tmpTuples[e.index]
			materialization.FetchTupleFromTmpTuplePage(e.context.bpm, &leftTuple, &leftTmpTuple)
		}
		if int(e.index) < len(e.tmpTuples) {
			// valid combination found
			retTuple := e.MakeOutputTuple(&leftTuple, &e.rightTuple)
			e.index++
			return retTuple, false, nil
		}
		// no valid combination, turn to the next right tuple by for loop
	}
}

func (e *HashJoinExecutor) IsValidCombination(leftTuple *tuple.Tuple, rightTuple *tuple.Tuple) bool {
	return e.plan.OnPredicate().EvaluateJoin(leftTuple, e.left.GetOutputSchema(), rightTuple, e.right.GetOutputSchema()).ToBoolean()
}

func (e *HashJoinExecutor) MakeOutputTuple(leftTuple *tuple.Tuple, rightTuple *tuple.Tuple) *tuple.Tuple {
	outputColumnCnt := int(e.GetOutputSchema().GetColumnCount())
	values := make([]types.Value, outputColumnCnt)
	for i := 0; i < outputColumnCnt; i++ {
		values[i] =
			e.outputExprs[i].EvaluateJoin(leftTuple, e.left.GetOutputSchema(), rightTuple, e.right.GetOutputSchema())
	}
	return tuple.NewTupleFromSchema(values, e.GetOutputSchema())
}

// can not be used
func (e *HashJoinExecutor) GetTableMetaData() *catalog.TableMetadata { return nil }

type SimpleHashJoinHashTable struct {
	hashTable map[uint32][]materialization.TmpTuple
}

func NewSimpleHashJoinHashTable() *SimpleHashJoinHashTable {
	return &SimpleHashJoinHashTable{hashTable: make(map[uint32][]materialization.TmpTuple)}
}

/**
 * Inserts a (hash key, tuple) pair into the hash table.
 * @param txn the transaction that we execute in
 * @param h the hash key
 * @param t the tuple to associate with the key
 * @return true if the insert succeeded
 */
func (jht *SimpleHashJoinHashTable) Insert(h uint32, t *materialization.TmpTuple) bool {
	if jht.hashTable[h] == nil {
		vals := make([]materialization.TmpTuple, 0)
		vals = append(vals, *t)
		jht.hashTable[h] = vals
	} else {
		jht.hashTable[h] = append(jht.hashTable[h], *t)
	}

	return true
}

/**
 * Gets the values in the hash table that match the given hash key.
 * @param txn the transaction that we execute in
 * @param h the hash key
 * @param[out] t the list of tuples that matched the key
 */
func (jht *SimpleHashJoinHashTable) GetValue(h uint32) []materialization.TmpTuple {
	return jht.hashTable[h]
}

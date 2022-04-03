package executors

/**
 * IdentityHashFunction hashes everything to itself, i.e. h(x) = x.
 */
 class IdentityHashFunction : public HashFunction<hash_t> {
	public:
	 /**
	  * Hashes the key.
	  * @param key the key to be hashed
	  * @return the hashed value
	  */
	 uint64_t GetHash(size_t key) override { return key; }
   };
   
   /**
	* A simple hash table that supports hash joins.
	*/
   class SimpleHashJoinHashTable {
	public:
	 /** Creates a new simple hash join hash table. */
	 SimpleHashJoinHashTable(const std::string &name, BufferPoolManager *bpm, HashComparator cmp, uint32_t buckets,
							 const IdentityHashFunction &hash_fn) {}
   
	 /**
	  * Inserts a (hash key, tuple) pair into the hash table.
	  * @param txn the transaction that we execute in
	  * @param h the hash key
	  * @param t the tuple to associate with the key
	  * @return true if the insert succeeded
	  */
	 bool Insert(Transaction *txn, hash_t h, const Tuple &t) {
	   hash_table_[h].emplace_back(t);
	   return true;
	 }
   
	 /**
	  * Gets the values in the hash table that match the given hash key.
	  * @param txn the transaction that we execute in
	  * @param h the hash key
	  * @param[out] t the list of tuples that matched the key
	  */
	 void GetValue(Transaction *txn, hash_t h, std::vector<Tuple> *t) { *t = hash_table_[h]; }
   
	private:
	 std::unordered_map<hash_t, std::vector<Tuple>> hash_table_;
   };
   
   // TODO(student): when you are ready to attempt task 3, replace the using declaration!
   // using HT = SimpleHashJoinHashTable;
   
   using HashJoinKeyType = Value;
   using HashJoinValType = TmpTuple;
   using HT = LinearProbeHashTable<HashJoinKeyType, HashJoinValType, ValueComparator>;
   
   /**
	* HashJoinExecutor executes hash join operations.
	*/
   class HashJoinExecutor : public AbstractExecutor {
	public:
	 /**
	  * Creates a new hash join executor.
	  * @param exec_ctx the context that the hash join should be performed in
	  * @param plan the hash join plan node
	  * @param left the left child, used by convention to build the hash table
	  * @param right the right child, used by convention to probe the hash table
	  */
	 HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan, std::unique_ptr<AbstractExecutor> &&left,
					  std::unique_ptr<AbstractExecutor> &&right)
		 : AbstractExecutor(exec_ctx),
		   plan_(plan),
		   jht_("hash join", exec_ctx_->GetBufferPoolManager(), jht_comp_, jht_num_buckets_, jht_hash_fn_),
		   left_(std::move(left)),
		   right_(std::move(right)) {}
   
	 /** @return the JHT in use. Do not modify this function, otherwise you will get a zero. */
	 const HT *GetJHT() const { return &jht_; }
   
	 const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }
   
	 void Init() override {
	   // get exprs to evaluate to output result
	   auto output_column_cnt = GetOutputSchema()->GetColumnCount();
	   output_exprs_.resize(output_column_cnt);
	   for (size_t i = 0; i < output_column_cnt; i++) {
		 output_exprs_[i] = GetOutputSchema()->GetColumn(i).GetExpr();
	   }
	   // build hash table from left
	   left_->Init();
	   right_->Init();
	   left_expr_ = plan_->Predicate()->GetChildAt(0);
	   right_expr_ = plan_->Predicate()->GetChildAt(1);
	   Tuple left_tuple;
	   // store all the left tuples in tmp pages in that it can not fit in memory
	   // use tmp tuple as the value of the hash table kv pair
	   TmpTuplePage *tmp_page = nullptr;
	   page_id_t tmp_page_id = INVALID_PAGE_ID;
	   TmpTuple tmp_tuple;
	   while (left_->Next(&left_tuple)) {
		 if (!tmp_page || !tmp_page->Insert(left_tuple,&tmp_tuple)) {
		   // unpin the last full tmp page
		   if (tmp_page_id != INVALID_PAGE_ID) exec_ctx_->GetBufferPoolManager()->UnpinPage(tmp_page_id,true);
		   // create new tmp page
		   tmp_page = reinterpret_cast<TmpTuplePage *>(exec_ctx_->GetBufferPoolManager()->NewPage(&tmp_page_id));
		   if (!tmp_page) throw std::runtime_error("fail to create new tmp page when doing hash join");
		   tmp_page->Init(tmp_page_id, PAGE_SIZE);
		   tmp_page_ids_.push_back(tmp_page_id);
		   // reinsert the tuple
		   assert(tmp_page->Insert(left_tuple, &tmp_tuple));
		 }
		 Value value = left_expr_->Evaluate(&left_tuple, left_->GetOutputSchema());
		 jht_.Insert(exec_ctx_->GetTransaction(), value, tmp_tuple);
	   }
	 }
   
	 bool Next(Tuple *tuple) override {
	   while (true) {
		 while (index_ == tmp_tuples_.size()) {
		   // we have traversed all possible join combination of the current right tuple
		   // move to the next right tuple
		   tmp_tuples_.clear();
		   index_ = 0;
		   if (!right_->Next(&right_tuple_)) {
			 // hash join finished, delete all the tmp page we created
			 for (auto tmp_page_id : tmp_page_ids_) {
			   exec_ctx_->GetBufferPoolManager()->DeletePage(tmp_page_id);
			 }
			 return false;
		   }
		   auto value = right_expr_->Evaluate(&right_tuple_, right_->GetOutputSchema());
		   jht_.GetValue(exec_ctx_->GetTransaction(), value, &tmp_tuples_);
		 }
		 // traverse corresponding left tuples stored in the tmp pages util we find one satisfying the predicate with current right tuple
		 auto left_tmp_tuple = tmp_tuples_[index_];
		 Tuple left_tuple;
		 FetchTupleFromTmpTuplePage(&left_tuple, left_tmp_tuple);
		 while (!IsValidCombination(left_tuple, right_tuple_)) {
		   index_++;
		   if (index_ == tmp_tuples_.size()) break;
		   left_tmp_tuple = tmp_tuples_[index_];
		   FetchTupleFromTmpTuplePage(&left_tuple, left_tmp_tuple);
		 }
		 if (index_ < tmp_tuples_.size()) {
		   // valid combination found
		   MakeOutputTuple(tuple,&left_tuple,&right_tuple_);
		   index_++;
		   return true;
		 }
		 // no valid combination, turn to the next right tuple by while loop
	   }
	 }
   
	 void FetchTupleFromTmpTuplePage(Tuple* tuple,const TmpTuple& tmp_tuple) {
	   auto tmp_page = reinterpret_cast<TmpTuplePage*>(exec_ctx_->GetBufferPoolManager()->FetchPage(tmp_tuple.GetPageId()));
	   if (!tmp_page) throw std::runtime_error("fail to fetch tmp page when doing hash join");
	   tmp_page->Get(tuple,tmp_tuple.GetOffset());
	   exec_ctx_->GetBufferPoolManager()->UnpinPage(tmp_tuple.GetPageId(),false);
	 }
   
	 bool IsValidCombination(const Tuple &left_tuple, const Tuple &right_tuple) {
	   return plan_->Predicate()
		   ->EvaluateJoin(&left_tuple, left_->GetOutputSchema(), &right_tuple, right_->GetOutputSchema())
		   .GetAs<bool>();
	 }
   
	 void MakeOutputTuple(Tuple *output_tuple, const Tuple *left_tuple, const Tuple *right_tuple) {
	   auto output_column_cnt = GetOutputSchema()->GetColumnCount();
	   vector<Value> values(output_column_cnt);
	   for (size_t i = 0; i < output_column_cnt; i++) {
		 values[i] =
			 output_exprs_[i]->EvaluateJoin(left_tuple, left_->GetOutputSchema(), right_tuple, right_->GetOutputSchema());
	   }
	   *output_tuple = Tuple(values, GetOutputSchema());
	 }
	 /**
	  * Hashes a tuple by evaluating it against every expression on the given schema, combining all non-null hashes.
	  * @param tuple tuple to be hashed
	  * @param schema schema to evaluate the tuple on
	  * @param exprs expressions to evaluate the tuple with
	  * @return the hashed tuple
	  */
	 hash_t HashValues(const Tuple *tuple, const Schema *schema, const std::vector<const AbstractExpression *> &exprs) {
	   hash_t curr_hash = 0;
	   // For every expression,
	   for (const auto &expr : exprs) {
		 // We evaluate the tuple on the expression and schema.
		 Value val = expr->Evaluate(tuple, schema);
		 // If this produces a value,
		 if (!val.IsNull()) {
		   // We combine the hash of that value into our current hash.
		   curr_hash = HashUtil::CombineHashes(curr_hash, HashUtil::HashValue(&val));
		 }
	   }
	   return curr_hash;
	 }
   
	private:
	 /** The hash join plan node. */
	 const HashJoinPlanNode *plan_;
	 /** The comparator is used to compare hashes. */
	 ValueComparator jht_comp_{};
	 /** The identity hash function. */
	 HashFunction<Value> jht_hash_fn_{};
   
	 /** The hash table that we are using. */
	 HT jht_;
	 /** The number of buckets in the hash table. */
	 static constexpr uint32_t jht_num_buckets_ = 2;
	 std::unique_ptr<AbstractExecutor> left_;
	 std::unique_ptr<AbstractExecutor> right_;
	 const AbstractExpression *left_expr_;
	 const AbstractExpression *right_expr_;
	 vector<TmpTuple> tmp_tuples_;
	 size_t index_ = 0;
	 vector<const AbstractExpression *> output_exprs_;
	 vector<page_id_t> tmp_page_ids_;
	 Tuple right_tuple_;
   };
package recovery

// /**
//  * CheckpointManager creates consistent checkpoints by blocking all other transactions temporarily.
//  */
//  class CheckpointManager {
// 	public:
// 	 CheckpointManager(TransactionManager *transaction_manager, LogManager *log_manager,
// 					   BufferPoolManager *buffer_pool_manager)
// 		 : transaction_manager_(transaction_manager),
// 		   log_manager_(log_manager),
// 		   buffer_pool_manager_(buffer_pool_manager) {}

// 	 ~CheckpointManager() = default;

// 	 void BeginCheckpoint();
// 	 void EndCheckpoint();

// 	private:
// 	 TransactionManager *transaction_manager_ __attribute__((__unused__));
// 	 LogManager *log_manager_ __attribute__((__unused__));
// 	 BufferPoolManager *buffer_pool_manager_ __attribute__((__unused__));
// };

// void CheckpointManager::BeginCheckpoint() {
// 	// Block all the transactions and ensure that both the WAL and all dirty buffer pool pages are persisted to disk,
// 	// creating a consistent checkpoint. Do NOT allow transactions to resume at the end of this method, resume them
// 	// in CheckpointManager::EndCheckpoint() instead. This is for grading purposes.
// 	transaction_manager_->BlockAllTransactions();
// 	buffer_pool_manager_->FlushAllPages();
// 	log_manager_->Flush();
// }

// void CheckpointManager::EndCheckpoint() {
// // Allow transactions to resume, completing the checkpoint.
// transaction_manager_->ResumeTransactions();
// }

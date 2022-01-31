package concurrency

import (
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
)

// TODO: (SDB) use CheckpointManager class ....
//             (c++ implementation does not use also...)
/**
 * CheckpointManager creates consistent checkpoints by blocking all other transactions temporarily.
 */
type CheckpointManager struct {
	transaction_manager *TransactionManager       //__attribute__((__unused__));
	log_manager         *recovery.LogManager      //__attribute__((__unused__));
	buffer_pool_manager *buffer.BufferPoolManager //__attribute__((__unused__));
}

func (checkpoint_manager *CheckpointManager) BeginCheckpoint() {
	// Block all the transactions and ensure that both the WAL and all dirty buffer pool pages are persisted to disk,
	// creating a consistent checkpoint. Do NOT allow transactions to resume at the end of this method, resume them
	// in CheckpointManager::EndCheckpoint() instead. This is for grading purposes.
	checkpoint_manager.transaction_manager.BlockAllTransactions()
	checkpoint_manager.buffer_pool_manager.FlushAllpages()
	checkpoint_manager.log_manager.Flush()
}

func (checkpoint_manager *CheckpointManager) EndCheckpoint() {
	// Allow transactions to resume, completing the checkpoint.
	checkpoint_manager.transaction_manager.ResumeTransactions()
}

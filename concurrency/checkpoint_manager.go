package concurrency

import (
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
)

/**
 * CheckpointManager creates consistent checkpoints by blocking all other transactions temporarily.
 */
type CheckpointManager struct {
	transaction_manager *access.TransactionManager //__attribute__((__unused__));
	log_manager         *recovery.LogManager       //__attribute__((__unused__));
	buffer_pool_manager *buffer.BufferPoolManager  //__attribute__((__unused__));
}

func NewCheckpointManager(
	transaction_manager *access.TransactionManager,
	log_manager *recovery.LogManager,
	buffer_pool_manager *buffer.BufferPoolManager) *CheckpointManager {
	return &CheckpointManager{transaction_manager, log_manager, buffer_pool_manager}
}

func (checkpoint_manager *CheckpointManager) BeginCheckpoint() {
	// Block all the transactions and ensure that both the WAL and all dirty buffer pool pages are persisted to disk,
	// creating a consistent checkpoint. Do NOT allow transactions to resume at the end of this method, resume them
	// in CheckpointManager::EndCheckpoint() instead. This is for grading purposes.
	checkpoint_manager.transaction_manager.BlockAllTransactions()
	checkpoint_manager.buffer_pool_manager.FlushAllPages()
	checkpoint_manager.log_manager.Flush()
}

func (checkpoint_manager *CheckpointManager) EndCheckpoint() {
	// Allow transactions to resume, completing the checkpoint.
	checkpoint_manager.transaction_manager.ResumeTransactions()
}

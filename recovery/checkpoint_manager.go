package recovery

import (
	"github.com/ryogrid/SamehadaDB/concurrency"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
)

/**
 * CheckpointManager creates consistent checkpoints by blocking all other transactions temporarily.
 */
type CheckpointManager struct {
	transaction_manager *concurrency.TransactinManager //__attribute__((__unused__));
	log_manager         *LogManager                    //__attribute__((__unused__));
	buffer_pool_manager *buffer.BufferPoolManager      //__attribute__((__unused__));
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

package concurrency

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/recovery"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"time"
)

/**
 * CheckpointManager creates consistent checkpoints by blocking all other transactions temporarily.
 */
type CheckpointManager struct {
	transaction_manager *access.TransactionManager
	log_manager         *recovery.LogManager
	buffer_pool_manager *buffer.BufferPoolManager
	// checkpointing thread works when this flag is true
	isCheckpointActive bool
}

func NewCheckpointManager(
	transaction_manager *access.TransactionManager,
	log_manager *recovery.LogManager,
	buffer_pool_manager *buffer.BufferPoolManager) *CheckpointManager {
	return &CheckpointManager{transaction_manager, log_manager, buffer_pool_manager, true}
}

func (checkpoint_manager *CheckpointManager) StartCheckpointTh() {
	go func() {
		for checkpoint_manager.IsCheckpointActive() {
			time.Sleep(time.Second * 30)
			if !checkpoint_manager.IsCheckpointActive() {
				break
			}
			fmt.Println("CheckpointTh: start checkpointing.")
			checkpoint_manager.BeginCheckpoint()
			checkpoint_manager.EndCheckpoint()
			fmt.Println("CheckpointTh: finish checkpointing.")
		}
	}()
}

func (checkpoint_manager *CheckpointManager) BeginCheckpoint() {
	// Block all the transactions and ensure that both the WAL and all dirty buffer pool pages are persisted to disk,
	// creating a consistent checkpoint. Do NOT allow transactions to resume at the end of this method, resume them
	// in CheckpointManager::EndCheckpoint() instead. This is for grading purposes.
	checkpoint_manager.transaction_manager.BlockAllTransactions()
	isSuccess := checkpoint_manager.buffer_pool_manager.FlushAllDirtyPages()
	if !isSuccess {
		fmt.Println("flush all dirty pages failed! at checkpointing (NOT BUG)")
	}
	checkpoint_manager.log_manager.Flush()
}

func (checkpoint_manager *CheckpointManager) EndCheckpoint() {
	// Allow transactions to resume, completing the checkpoint.
	checkpoint_manager.transaction_manager.ResumeTransactions()
}

func (checkpoint_manager *CheckpointManager) StopCheckpointTh() {
	checkpoint_manager.isCheckpointActive = false
}

func (checkpoint_manager *CheckpointManager) IsCheckpointActive() bool {
	return checkpoint_manager.isCheckpointActive
}

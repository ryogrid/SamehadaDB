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
	transactionManager *access.TransactionManager
	logManager         *recovery.LogManager
	bufferPoolManager *buffer.BufferPoolManager
	// checkpointing thread works when this flag is true
	isCheckpointActive bool
}

func NewCheckpointManager(
	transactionManager *access.TransactionManager,
	logManager *recovery.LogManager,
	bufferPoolManager *buffer.BufferPoolManager) *CheckpointManager {
	return &CheckpointManager{transactionManager, logManager, bufferPoolManager, true}
}

func (cm *CheckpointManager) StartCheckpointTh() {
	go func() {
		for cm.IsCheckpointActive() {
			time.Sleep(time.Second * 30)
			if !cm.IsCheckpointActive() {
				break
			}
			fmt.Println("CheckpointTh: start checkpointing.")
			cm.BeginCheckpoint()
			cm.EndCheckpoint()
			fmt.Println("CheckpointTh: finish checkpointing.")
		}
	}()
}

func (cm *CheckpointManager) BeginCheckpoint() {
	// Block all the transactions and ensure that both the WAL and all dirty buffer pool pages are persisted to disk,
	// creating a consistent checkpoint. Do NOT allow transactions to resume at the end of this method, resume them
	// in CheckpointManager::EndCheckpoint() instead. This is for grading purposes.
	cm.transactionManager.BlockAllTransactions()
	isSuccess := cm.bufferPoolManager.FlushAllDirtyPages()
	if !isSuccess {
		fmt.Println("flush all dirty pages failed! at checkpointing (NOT BUG)")
	}
	cm.logManager.Flush()
}

func (cm *CheckpointManager) EndCheckpoint() {
	// Allow transactions to resume, completing the checkpoint.
	cm.transactionManager.ResumeTransactions()
}

func (cm *CheckpointManager) StopCheckpointTh() {
	cm.isCheckpointActive = false
}

func (cm *CheckpointManager) IsCheckpointActive() bool {
	return cm.isCheckpointActive
}

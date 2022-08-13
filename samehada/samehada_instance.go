package samehada

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/concurrency"
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/disk"
)

type SamehadaInstance struct {
	disk_manager        disk.DiskManager
	log_manager         *recovery.LogManager
	bpm                 *buffer.BufferPoolManager
	lock_manager        *access.LockManager
	transaction_manager *access.TransactionManager
	checkpoint_manger   *concurrency.CheckpointManager
}

func NewSamehadaInstanceForTesting() *SamehadaInstance {
	ret := NewSamehadaInstance("test", common.BufferPoolMaxFrameNumForTest)
	common.EnableLogging = false
	return ret
}

// reset program state except for variables on testcase function
// and db/log file
// bpoolSize: usable buffer size in frame(=page) num
func NewSamehadaInstance(dbName string, bpoolSize int) *SamehadaInstance {
	common.EnableLogging = true

	disk_manager := disk.NewDiskManagerImpl(dbName + ".db")
	log_manager := recovery.NewLogManager(&disk_manager)
	bpm := buffer.NewBufferPoolManager(uint32(bpoolSize), disk_manager, log_manager)
	lock_manager := access.NewLockManager(access.STRICT, access.SS2PL_MODE)
	transaction_manager := access.NewTransactionManager(lock_manager, log_manager)
	checkpoint_manager := concurrency.NewCheckpointManager(transaction_manager, log_manager, bpm)

	return &SamehadaInstance{disk_manager, log_manager, bpm, lock_manager, transaction_manager, checkpoint_manager}
}

func (si *SamehadaInstance) GetDiskManager() disk.DiskManager {
	return si.disk_manager
}

func (si *SamehadaInstance) GetLogManager() *recovery.LogManager {
	return si.log_manager
}

func (si *SamehadaInstance) GetBufferPoolManager() *buffer.BufferPoolManager {
	return si.bpm
}

func (si *SamehadaInstance) GetLockManager() *access.LockManager {
	return si.lock_manager
}

func (si *SamehadaInstance) GetTransactionManager() *access.TransactionManager {
	return si.transaction_manager
}

func (si *SamehadaInstance) GetCheckpointManager() *concurrency.CheckpointManager {
	return si.checkpoint_manger
}

// functionality is Flushing dirty pages, shutdown of DiskManager and action around DB/Log files
func (si *SamehadaInstance) Shutdown(IsRemoveFiles bool) {
	// TODO: (SDB) flush all dirty pages
	si.disk_manager.ShutDown()
	if IsRemoveFiles {
		si.disk_manager.RemoveDBFile()
		si.disk_manager.RemoveLogFile()
	}
}

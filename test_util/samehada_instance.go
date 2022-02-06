package test_util

import (
	"unsafe"

	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/disk"
)

type SamehadaInstance struct {
	disk_manager        *disk.DiskManager
	log_manager         *recovery.LogManager
	bpm                 *buffer.BufferPoolManager
	lock_manager        *access.LockManager
	transaction_manager *access.TransactionManager
}

func NewSamehadaInstance() *SamehadaInstance {
	disk_manager := disk.NewDiskManagerImpl("test.db")
	log_manager := recovery.NewLogManager(&disk_manager)
	bpm := buffer.NewBufferPoolManager(uint32(32), disk_manager)
	lock_manager := access.NewLockManager(access.REGULAR, access.PREVENTION)
	transaction_manager := access.NewTransactionManager(log_manager)
	return &SamehadaInstance{&disk_manager, log_manager, bpm, lock_manager, transaction_manager}
}

func (si *SamehadaInstance) GetDiskManager() *disk.DiskManager {
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

// do shutdown of DiskManager only
func (si *SamehadaInstance) Finalize() {
	((*disk.DiskManagerImpl)(unsafe.Pointer(si.disk_manager))).ShutDown()
}

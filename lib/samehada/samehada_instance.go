package samehada

import (
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/concurrency"
	"github.com/ryogrid/SamehadaDB/lib/recovery"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/disk"
)

type ShutdownPattern int

const (
	ShutdownPatternRemoveFiles ShutdownPattern = iota
	ShutdownPatternCloseFiles
)

type SamehadaInstance struct {
	diskManager        disk.DiskManager
	logManager         *recovery.LogManager
	bpm                *buffer.BufferPoolManager
	lockManager        *access.LockManager
	transactionManager *access.TransactionManager
	checkpointManager  *concurrency.CheckpointManager
}

func NewSamehadaInstanceForTesting() *SamehadaInstance {
	ret := NewSamehadaInstance("test", common.BufferPoolMaxFrameNumForTest)
	ret.GetLogManager().DeactivateLogging()
	return ret
}

// reset program state except for variables on testcase function
// and db/log file
// bpoolSize: usable buffer size in frame(=page) num
func NewSamehadaInstance(dbName string, bpoolSize int) *SamehadaInstance {
	var dman disk.DiskManager
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage {
		dman = disk.NewDiskManagerImpl(dbName + ".db")
	} else {
		dman = disk.NewVirtualDiskManagerImpl(dbName + ".db")
	}

	logMgr := recovery.NewLogManager(&dman)
	logMgr.ActivateLogging()
	bpm := buffer.NewBufferPoolManager(uint32(bpoolSize), dman, logMgr)
	lockMgr := access.NewLockManager(access.STRICT, access.SS2PLMode)
	txnMgr := access.NewTransactionManager(lockMgr, logMgr)
	cpMgr := concurrency.NewCheckpointManager(txnMgr, logMgr, bpm)

	return &SamehadaInstance{dman, logMgr, bpm, lockMgr, txnMgr, cpMgr}
}

func (si *SamehadaInstance) GetDiskManager() disk.DiskManager {
	return si.diskManager
}

func (si *SamehadaInstance) GetLogManager() *recovery.LogManager {
	return si.logManager
}

func (si *SamehadaInstance) GetBufferPoolManager() *buffer.BufferPoolManager {
	return si.bpm
}

func (si *SamehadaInstance) GetLockManager() *access.LockManager {
	return si.lockManager
}

func (si *SamehadaInstance) GetTransactionManager() *access.TransactionManager {
	return si.transactionManager
}

func (si *SamehadaInstance) GetCheckpointManager() *concurrency.CheckpointManager {
	return si.checkpointManager
}

// functionality is Flushing dirty pages, shutdown of DiskManager and action around DB/Log files
func (si *SamehadaInstance) Shutdown(shutdownPat ShutdownPattern) {
	switch shutdownPat {
	case ShutdownPatternRemoveFiles:
		//close
		si.diskManager.ShutDown()
		//remove
		si.diskManager.RemoveDBFile()
		si.diskManager.RemoveLogFile()
	case ShutdownPatternCloseFiles:
		si.logManager.Flush()
		// TODO: (SDB) need to finalize BTreeIndex objects
		si.bpm.FlushAllDirtyPages()
		logRecord := recovery.NewLogRecordGracefulShutdown()
		si.logManager.AppendLogRecord(logRecord)
		si.logManager.Flush()
		// close only
		si.diskManager.ShutDown()
	default:
		panic("invalid shutdown pattern")
	}
}

// for testing. this method does file closing only in contrast to Shutdown method
func (si *SamehadaInstance) CloseFilesForTesting() {
	si.diskManager.ShutDown()
}

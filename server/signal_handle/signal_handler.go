package signal_handle

import (
	"github.com/ryogrid/SamehadaDB/lib/samehada"
	"os"
	"os/signal"
	"syscall"
)

var IsStopped = false

func SignalHandlerTh(db *samehada.SamehadaDB, exitNotifyCh *chan bool) {
	sigChan := make(chan os.Signal, 1)
	// receive SIGINT only
	signal.Ignore()
	signal.Notify(sigChan, syscall.SIGINT)

	// block until receive SIGINT
	<-sigChan

	// ---- after receive SIGINT ---

	// stop handle request
	IsStopped = true

	// shutdown SamehadaDB object
	db.Shutdown()

	// notify that shutdown operation finished to main thread
	*exitNotifyCh <- true
}

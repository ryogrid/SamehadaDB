package samehada

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/common"
	"sync"
)

type queryRequest struct {
	reqId    *uint64
	queryStr *string
	callerCh *chan *reqResult
}

type RequestManager struct {
	sdb               *SamehadaDB
	nextReqId         uint64
	execQue           []*queryRequest
	queMutex          *sync.Mutex
	curExectingReqNum uint64
	inCh              *chan *reqResult
	isExecutionActive bool
}

func NewRequestManager(sdb *SamehadaDB) *RequestManager {
	//ch := make(chan *reqResult, 1000000)
	ch := make(chan *reqResult, 100)
	//ch := make(chan *reqResult)
	return &RequestManager{sdb, 0, make([]*queryRequest, 0), new(sync.Mutex), 0, &ch, true}
}

func (reqManager *RequestManager) AppendRequest(queryStr *string) *chan *reqResult {
	reqManager.queMutex.Lock()

	qr := new(queryRequest)
	tmpId := reqManager.nextReqId
	qr.reqId = &tmpId
	reqManager.nextReqId++
	qr.queryStr = queryStr

	retCh := make(chan *reqResult)

	qr.callerCh = &retCh

	reqManager.execQue = append(reqManager.execQue, qr)
	reqManager.queMutex.Unlock()

	// wake up execution thread
	*reqManager.inCh <- nil

	return &retCh
}

// caller must having lock of queMutex
func (reqManager *RequestManager) RetrieveRequest() *queryRequest {
	retVal := reqManager.execQue[0]
	reqManager.execQue = reqManager.execQue[1:]
	return retVal
}

func (reqManager *RequestManager) StartTh() {
	go reqManager.Run()
}

func (reqManager *RequestManager) StopTh() {
	reqManager.isExecutionActive = false
	*reqManager.inCh <- nil
}

// caller must having lock of queMutex
func (reqManager *RequestManager) executeQuedTxns() {
	qr := reqManager.RetrieveRequest()
	go reqManager.sdb.ExecuteSQLForTxnTh(reqManager.inCh, qr)
	reqManager.curExectingReqNum++
}

// caller must having lock of queMutex
func (reqManager *RequestManager) handleAbortedByCCTxn(result *reqResult) {
	// insert aborted request to head of que
	reqManager.execQue = append([]*queryRequest{&queryRequest{result.reqId, result.query, result.callerCh}}, reqManager.execQue...)
	// TODO: (SDB) [PARA] for debug
	fmt.Println("add que aborted req")
}

func (reqManager *RequestManager) Run() {
	for {
		recvVal := <-*reqManager.inCh
		if recvVal != nil { // receive result
			reqManager.queMutex.Lock()
			reqManager.curExectingReqNum--

			if recvVal.err != nil {
				if recvVal.err == QueryAbortedErr {
					reqManager.handleAbortedByCCTxn(recvVal)
					reqManager.queMutex.Unlock()
				} else {
					reqManager.queMutex.Unlock()
					*recvVal.callerCh <- recvVal
				}
			} else {
				reqManager.queMutex.Unlock()
				*recvVal.callerCh <- recvVal
			}
		}

		// check stop signal or new request
		if !reqManager.isExecutionActive {
			break
		}
		reqManager.queMutex.Lock()
		if len(reqManager.execQue) > 0 && reqManager.curExectingReqNum < common.MaxTxnThreadNum {
			reqManager.executeQuedTxns()
		}
		reqManager.queMutex.Unlock()
	}
}

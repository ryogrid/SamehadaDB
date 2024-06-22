package skip_list_bench

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/container/skip_list"
	"github.com/ryogrid/SamehadaDB/lib/samehada"
	"github.com/ryogrid/SamehadaDB/lib/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

const INITIAL_VAL_NUM = 300000
const WORK_NUM = INITIAL_VAL_NUM / 10

type opTypeAndVal struct {
	OpType skip_list.SkipListOpType
	Val    *types.Value
}

type workArray struct {
	arr        [WORK_NUM]*opTypeAndVal
	pos        int32
	posForInit int32
	mutex      sync.Mutex
}

func (arr *workArray) GetNewWork(threadNum int32) (work []*opTypeAndVal, done bool) {
	arr.mutex.Lock()
	defer arr.mutex.Unlock()
	splitedWorkNum := WORK_NUM / threadNum

	if arr.pos+splitedWorkNum <= WORK_NUM {
		retArr := arr.arr[arr.pos : arr.pos+splitedWorkNum]
		arr.pos = arr.pos + splitedWorkNum
		return retArr, false
	} else {
		return nil, true
	}

}

func NewWorkArray() *workArray {
	ret := new(workArray)
	ret.pos = 0
	ret.posForInit = -1
	return ret
}

func (arr *workArray) Append(val *types.Value) {
	arr.posForInit++
	randVal := rand.Intn(10)
	if randVal < 2 {
		arr.arr[arr.posForInit] = &opTypeAndVal{skip_list.SKIP_LIST_OP_REMOVE, val}
	} else {
		arr.arr[arr.posForInit] = &opTypeAndVal{skip_list.SKIP_LIST_OP_GET, val}
	}
}

func (arr *workArray) Shuffle() {
	rand.Shuffle(len(arr.arr), func(i, j int) { arr.arr[i], arr.arr[j] = arr.arr[j], arr.arr[i] })
}

func TestSkipListBench8_2(t *testing.T) {
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}

	runtime.GOMAXPROCS(50)

	threadNumArr := []int{1, 2, 3, 4, 5, 6, 12, 20, 50, 100}

	masterCh := make(chan int)
	// measure in each thread num
	for ii := 0; ii < 10; ii++ {
		sl, wArray := genInitialSLAndWorkArr(t.Name())
		fmt.Println("setuped data.")
		threadNum := threadNumArr[ii]
		chanArr := make([]chan int, 0)
		for jj := 0; jj < threadNum; jj++ {
			ch := make(chan int)
			chanArr = append(chanArr, ch)
			go func(startCh chan int) {
				<-startCh
				for {
					work, done := wArray.GetNewWork(int32(threadNum))
					if done {
						masterCh <- 1
						break
					}
					for _, wk := range work {
						switch wk.OpType {
						case skip_list.SKIP_LIST_OP_REMOVE:
							sl.Remove(wk.Val, uint64(wk.Val.ToInteger()))
						case skip_list.SKIP_LIST_OP_GET:
							sl.GetValue(wk.Val)
						default:
							panic("illegal operation")
						}
					}
				}
			}(ch)
		}
		// lauched thread start operation
		for jj := 0; jj < threadNum; jj++ {
			chanArr[jj] <- 1
		}
		fmt.Println("start measure.")
		startTime := time.Now()
		// wait finish of threads
		for jj := 0; jj < threadNum; jj++ {
			<-masterCh
		}
		d := time.Since(startTime)
		fmt.Printf("threadNum=%d: elapsed %v\n", threadNum, d)
	}
}

func genInitialSLAndWorkArr(dbName string) (*skip_list.SkipList, *workArray) {
	rand.Seed(5)

	//shi := samehada.NewSamehadaInstance(dbName, 2000) //cover about 10% filled data
	shi := samehada.NewSamehadaInstance(dbName, 4000) //cover 100% of filled data
	bpm := shi.GetBufferPoolManager()

	sl := skip_list.NewSkipList(bpm, types.Integer, nil)
	wArray := NewWorkArray()

	// insert initial values and fill work array
	for ii := 0; ii < INITIAL_VAL_NUM; ii++ {
		tmpValBase := ii
		tmpVal := samehada_util.GetPonterOfValue(types.NewInteger(int32(tmpValBase)))
		sl.Insert(tmpVal, uint64(tmpValBase))
		if ii%WORK_NUM == 0 {
			fmt.Printf("genInitialSLAndWorkArr: %d entries inserted.\n", ii)
		}
		if ii < WORK_NUM {
			wArray.Append(tmpVal)
		}
	}
	wArray.Shuffle()
	return sl, wArray
}

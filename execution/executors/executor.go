package executors

import (
	"github.com/brunocalza/go-bustub/storage/page"
	"github.com/brunocalza/go-bustub/storage/table"
)

type Executor interface {
	Init()
	Next() (*table.Tuple, *page.RID)
}

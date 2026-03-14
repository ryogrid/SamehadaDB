package concurrency

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"time"
)

type StatisticsUpdater struct {
	transactionManager *access.TransactionManager
	c                   *catalog.Catalog
	// updater thread works when this flag is true
	isUpdaterActive bool
}

func NewStatisticsUpdater(
	transactionManager *access.TransactionManager, c *catalog.Catalog) *StatisticsUpdater {

	return &StatisticsUpdater{transactionManager, c, true}
}

func (updater *StatisticsUpdater) StartStaticsUpdaterTh() {
	go func() {
		for updater.IsUpdaterActive() {
			if !updater.IsUpdaterActive() {
				break
			}
			fmt.Println("StatisticsUpdaterTh: start updating.")
			updater.BeginStatsUpdate()
			updater.UpdateAllTablesStatistics()
			updater.EndStatsUpdate()
			fmt.Println("StatisticsUpdaterTh: finish updating.")
			time.Sleep(time.Second * 10)
		}
	}()
}

func (updater *StatisticsUpdater) UpdateAllTablesStatistics() {
	txn := updater.transactionManager.Begin(nil)
	defer updater.transactionManager.Commit(updater.c, txn)

	tables := updater.c.GetAllTables()
	for _, tbl := range tables {
		stat := tbl.GetStatistics()
		err := stat.Update(tbl, txn)
		if err != nil {
			// note: already updated table's statistics are not rollbacked
			//       in current impl
			return
		}
	}
}

func (updater *StatisticsUpdater) BeginStatsUpdate() {
	// do nothing
}

func (updater *StatisticsUpdater) EndStatsUpdate() {
	// do nothing
}

func (updater *StatisticsUpdater) StopStatsUpdateTh() {
	updater.isUpdaterActive = false
}

func (updater *StatisticsUpdater) IsUpdaterActive() bool {
	return updater.isUpdaterActive
}

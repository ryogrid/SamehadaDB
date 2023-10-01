package concurrency

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"time"
)

type StatisticsUpdater struct {
	transaction_manager *access.TransactionManager
	c                   *catalog.Catalog
	// updater thread works when this flag is true
	isUpdaterActive bool
}

func NewStatisticsUpdater(
	transaction_manager *access.TransactionManager, c *catalog.Catalog) *StatisticsUpdater {

	return &StatisticsUpdater{transaction_manager, c, true}
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
	txn := updater.transaction_manager.Begin(nil)
	defer updater.transaction_manager.Commit(updater.c, txn)

	tables := updater.c.GetAllTables()
	for _, table_ := range tables {
		stat := table_.GetStatistics()
		err := stat.Update(table_, txn)
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

func (updater *StatisticsUpdater) StopCheckpointTh() {
	updater.isUpdaterActive = false
}

func (updater *StatisticsUpdater) IsUpdaterActive() bool {
	return updater.isUpdaterActive
}

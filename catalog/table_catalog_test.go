package catalog

import (
	"testing"
)

func TestTableCatalog(t *testing.T) {
	// diskManager := disk.NewDiskManagerImpl("test.db")
	// defer diskManager.ShutDown()
	// bpm := buffer.NewBufferPoolManager(uint32(32), diskManager)

	// // catalog := BootstrapCatalog(bpm)

	// // columnA := table.NewColumn("a", types.Integer)
	// // columnB := table.NewColumn("b", types.Integer)
	// // schema := table.NewSchema([]*table.Column{columnA, columnB})

	// // catalog.CreateTable("test_1", schema)
	// // bpm.FlushAllpages()

	// catalog := GetCatalog(bpm)

	// fmt.Println(catalog.GetTableByOID(0).Schema().GetColumn(1))
}

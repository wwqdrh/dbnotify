package structhandler

import (
	"datamanager/server/common/driver"
	system_model "datamanager/server/model/system"
)

func ExampleListTable() {
	system_model.InitDB("postgres", "host=office.zx-tech.net user=postgres password=postgres dbname=postgres port=5435 sslmode=disable TimeZone=Asia/Shanghai")
	driver := new(driver.PostgresDriver).InitWithDB(system_model.DB())
	handler := NewBaseStructHandler(driver)
	handler.GetTables()

	// Output:
}

func ExampleListFields() {
	system_model.InitDB("postgres", "host=office.zx-tech.net user=postgres password=postgres dbname=postgres port=5435 sslmode=disable TimeZone=Asia/Shanghai")
	driver := new(driver.PostgresDriver).InitWithDB(system_model.DB())
	handler := NewBaseStructHandler(driver)
	handler.GetFields("data_1")

	// Output:
}

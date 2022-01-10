package plugger

import (
	"datamanager/model"
)

func ExampleListTable() {
	model.InitDB("postgres", "host=office.zx-tech.net user=postgres password=postgres dbname=postgres port=5435 sslmode=disable TimeZone=Asia/Shanghai")
	driver := new(PostgresDriver).InitWithDB(model.DB())
	driver.ListTable()

	// Output:
}

func ExampleListTrigger() {
	model.InitDB("postgres", "host=office.zx-tech.net user=postgres password=postgres dbname=postgres port=5435 sslmode=disable TimeZone=Asia/Shanghai")
	driver := new(PostgresDriver).InitWithDB(model.DB())
	driver.ListTrigger()

	// Output:
}

func ExampleListField() {
	model.InitDB("postgres", "host=office.zx-tech.net user=postgres password=postgres dbname=resmgr_table port=5433 sslmode=disable TimeZone=Asia/Shanghai")
	driver := new(PostgresDriver).InitWithDB(model.DB())
	driver.ListTableField("data_1")

	// Output:
}

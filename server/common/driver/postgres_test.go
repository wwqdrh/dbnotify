package driver

import system_model "datamanager/server/model/system"

func ExampleListTable() {
	system_model.InitDB("postgres", "host=office.zx-tech.net user=postgres password=postgres dbname=postgres port=5435 sslmode=disable TimeZone=Asia/Shanghai")
	driver := new(PostgresDriver).InitWithDB(system_model.DB())
	driver.ListTable()

	// Output:
}

func ExampleListTrigger() {
	system_model.InitDB("postgres", "host=office.zx-tech.net user=postgres password=postgres dbname=postgres port=5435 sslmode=disable TimeZone=Asia/Shanghai")
	driver := new(PostgresDriver).InitWithDB(system_model.DB())
	driver.ListTrigger()

	// Output:
}

func ExampleListField() {
	system_model.InitDB("postgres", "host=office.zx-tech.net user=postgres password=postgres dbname=resmgr_table port=5433 sslmode=disable TimeZone=Asia/Shanghai")
	driver := new(PostgresDriver).InitWithDB(system_model.DB())
	driver.ListTableField("data_1")

	// Output:
}

package driver

import system_model "datamanager/server/model/system"

func ExampleListTrigger() {
	system_model.InitDB("postgres", "host=office.zx-tech.net user=postgres password=postgres dbname=postgres port=5435 sslmode=disable TimeZone=Asia/Shanghai")
	driver := new(PostgresDriver).InitWithDB(system_model.DB())
	driver.ListTrigger()

	// Output:
}

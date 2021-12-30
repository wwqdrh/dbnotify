package driver

import (
	"datamanager/pkg/plugger/postgres"
	"sync"
)

// postgres的driver 单例
var PostgresDriver *postgres.PostgresDriver
var postgresOnce = sync.Once{}

// 初始化driver
func InitPostgresDriver() (e error) {
	postgresOnce.Do(func() {
		driver, err := postgres.NewPostgresDriver("postgres", "postgres", "office.zx-tech.net:5433", "hui_dm_test")
		if err != nil {
			e = err
		} else {
			driver.Initalial("hui_dm_test")
			PostgresDriver = driver
		}
	})
	return
}

package driver

import (
	"datamanager/pkg/plugger/postgres"
	"sync"
)

// postgres的driver 单例
var PostgresDriver *postgres.PostgresDriver
var postgresOnce = sync.Once{}

// 初始化driver
func InitPostgresDriver(config *postgres.PostgresConfig) (e error) {
	postgresOnce.Do(func() {
		driver, err := postgres.NewPostgresDriverWithConfig(config)
		if err != nil {
			e = err
		} else {
			driver.Initalial("hui_dm_test")
			PostgresDriver = driver
		}
	})
	return
}

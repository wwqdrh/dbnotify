package common

import (
	"datamanager/common/driver"
	"datamanager/core/manager"
	"sync"
)

var (
	Mana        *manager.ManagerCore
	managerOnce = sync.Once{}
)

func InitApp() (errs []error) {
	// 初始化连接器
	errs = driver.InitDriver()
	if len(errs) > 0 {
		return
	}

	// manager初始化
	managerOnce.Do(func() {
		Mana = manager.NewManagerCore(driver.PostgresDriver, driver.SqliteDriver.DB)
	})
	Mana.AutoMigrate()
	return
}

//
func Register(table interface{}) error {
	// 构建新的表
	return Mana.Register(table)
}

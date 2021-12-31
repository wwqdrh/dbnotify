package common

import (
	"datamanager/common/driver"
	"datamanager/core/manager"
	"datamanager/pkg/plugger/postgres"
	"sync"
)

var (
	Mana        *manager.ManagerCore // 处理日志问题的核心类
	managerOnce = sync.Once{}
)

func InitApp(targetDB *postgres.PostgresConfig) (errs []error) {
	// 初始化连接器
	errs = driver.InitDriver(targetDB)
	if len(errs) > 0 {
		return
	}

	// manager初始化
	managerOnce.Do(func() {
		conf := (&manager.ManagerConf{
			TargetDB:  driver.PostgresDriver,
			VersionDB: driver.SqliteDriver,
			LogDB:     driver.LevelDBDriver,
			OutDate:   15,
		}).Init()
		Mana = manager.NewManagerCore(conf)
	})
	Mana.AutoMigrate()
	return
}

func AddTable(table interface{}) error {
	return Mana.Register(table)
}

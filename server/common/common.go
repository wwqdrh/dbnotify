package common

import (
	"datamanager/server/manager"
	"datamanager/server/types"

	"gorm.io/gorm"
)

var (
	Mana *manager.ManagerCore // 处理日志问题的核心类
	conf *manager.ManagerConf
)

func InitApp(targetDB *gorm.DB, logPath string, handler types.IStructHandler, tables ...interface{}) (errs []error) {
	// 初始化连接器
	errs = InitDriver(targetDB, logPath)
	if len(errs) > 0 {
		return
	}
	// manager初始化
	conf = (&manager.ManagerConf{
		TargetDB:           PostgresDriver,
		LogDB:              LevelDBDriver,
		OutDate:            15,
		MinLogNum:          10,
		TableStructHandler: handler,
	}).Init()
	Mana = manager.NewManagerCore(conf).Init()

	for _, table := range tables {
		Mana.Register(table, conf.MinLogNum, conf.OutDate, nil, nil)
	}

	return
}

func AddTable(table interface{}, senseFields []string, ignoreFields []string) {
	Mana.Register(table, conf.MinLogNum, conf.OutDate, senseFields, ignoreFields)
}
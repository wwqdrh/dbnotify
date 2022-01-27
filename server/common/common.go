package common

import (
	"datamanager/server/service/dblog"
	"datamanager/server/types"
)

var (
	Mana *dblog.ManagerCore // 处理日志问题的核心类
	conf *dblog.ManagerConf
)

func InitApp(handler types.IStructHandler, tables ...interface{}) (errs []error) {
	// manager初始化
	conf = (&dblog.ManagerConf{
		OutDate:            15,
		MinLogNum:          10,
		TableStructHandler: handler,
	}).Init()
	Mana = dblog.NewManagerCore(conf).Init()

	for _, table := range tables {
		Mana.Register(table, conf.MinLogNum, conf.OutDate, nil, nil)
	}

	return
}

func AddTable(table interface{}, senseFields []string, ignoreFields []string) {
	Mana.Register(table, conf.MinLogNum, conf.OutDate, senseFields, ignoreFields)
}

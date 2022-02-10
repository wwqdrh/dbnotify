package dblog

import "github.com/wwqdrh/datamanager/global"

var ServiceGroupApp *ServiceGroup

type ServiceGroup struct {
	Logger *loggerService
	Meta   *MetaService
}

func InitService() {
	ServiceGroupApp = &ServiceGroup{
		Logger: new(loggerService).Init(global.G_CONFIG.DataLog.LogTableName, global.G_StructHandler),
		Meta:   new(MetaService).Init(),
	}

	// 初始化，例如创建必要的表，事件触发器等
	// if err := ServiceGroupApp.Meta.InitialDB(); err != nil {
	// 	panic(err)
	// }
}

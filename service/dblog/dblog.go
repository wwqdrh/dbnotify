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
}

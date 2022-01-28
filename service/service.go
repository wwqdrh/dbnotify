package service

import "github.com/wwqdrh/datamanager/service/dblog"

var ServiceGroupApp *ServiceGroup

type ServiceGroup struct {
	Dblog *dblog.ServiceGroup
}

func InitService() {
	dblog.InitService()
	ServiceGroupApp = &ServiceGroup{
		Dblog: dblog.ServiceGroupApp,
	}
}

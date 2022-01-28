package service

import "datamanager/server/service/dblog"

var ServiceGroupApp *ServiceGroup

type ServiceGroup struct {
	Dblog *dblog.ServiceGroup
}

func init() {
	ServiceGroupApp = &ServiceGroup{
		Dblog: dblog.ServiceGroupApp,
	}
}

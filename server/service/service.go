package service

import "datamanager/server/service/dblog"

type ServiceGroup struct {
	dblog.DataManagerService
}

var ServiceGroupApp = new(ServiceGroup)

package core

import "datamanager/server/service"

func InitService() CoreOption {
	return func() error {
		service.InitService()
		return nil
	}
}

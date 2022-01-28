package core

import "github.com/wwqdrh/datamanager/service"

func InitService() CoreOption {
	return func() error {
		service.InitService()
		return nil
	}
}

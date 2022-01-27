package core

import (
	"datamanager/server/common/datautil"
	"datamanager/server/global"
)

func InitTaskQueue() CoreOption {
	return func() error {
		global.G_LogTaskQueue = datautil.NewQueue()

		return nil
	}
}

package core

import (
	"github.com/wwqdrh/datamanager/common/datautil"
	"github.com/wwqdrh/datamanager/global"
)

func InitTaskQueue() CoreOption {
	return func() error {
		global.G_LogTaskQueue = datautil.NewQueue()

		return nil
	}
}

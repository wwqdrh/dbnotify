package core

import (
	"github.com/wwqdrh/datamanager/internal/datautil"
)

func InitTaskQueue() CoreOption {
	return func() error {
		G_LogTaskQueue = datautil.NewQueue()

		return nil
	}
}

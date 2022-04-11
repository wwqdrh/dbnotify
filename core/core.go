package core

import (
	"github.com/wwqdrh/datamanager/internal/datautil"
	"github.com/wwqdrh/datamanager/internal/driver"
	"github.com/wwqdrh/datamanager/internal/structhandler"

	"golang.org/x/sync/singleflight"
)

const (
	ConfigEnv  = "GVA_CONFIG"
	ConfigFile = "config.yaml"
	ConfigTemp = true
)

var (
	G_Concurrency_Control = &singleflight.Group{}

	G_DATADB *driver.PostgresDriver
	G_LOGDB  *driver.LevelDBDriver

	G_CONFIG        AllConfig
	G_LogTaskQueue  *datautil.Queue
	G_StructHandler structhandler.IStructHandler
)

type CoreOption func() error

func Init(o ...CoreOption) {
	var err error
	for _, i := range o {
		if err = i(); err != nil {
			panic(err)
		}
	}
}

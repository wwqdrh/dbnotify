package global

import (
	"github.com/wwqdrh/datamanager/common/datautil"
	"github.com/wwqdrh/datamanager/common/driver"
	"github.com/wwqdrh/datamanager/common/structhandler"
	"github.com/wwqdrh/datamanager/config"

	"github.com/spf13/viper"
	"golang.org/x/sync/singleflight"
)

var (
	G_VP                  *viper.Viper
	G_Concurrency_Control = &singleflight.Group{}

	G_DATADB *driver.PostgresDriver
	G_LOGDB  *driver.LevelDBDriver

	G_CONFIG        config.AllConfig
	G_LogTaskQueue  *datautil.Queue
	G_StructHandler structhandler.IStructHandler
)

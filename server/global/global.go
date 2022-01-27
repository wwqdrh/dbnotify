package global

import (
	"datamanager/server/common/datautil"
	"datamanager/server/common/driver"
	"datamanager/server/config"

	"github.com/spf13/viper"
	"golang.org/x/sync/singleflight"
)

var (
	G_VP                  *viper.Viper
	G_Concurrency_Control = &singleflight.Group{}

	G_DATADB *driver.PostgresDriver
	G_LOGDB  *driver.LevelDBDriver

	G_CONFIG       config.AllConfig
	G_LogTaskQueue *datautil.Queue
)

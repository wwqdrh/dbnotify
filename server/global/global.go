package global

import (
	"datamanager/server/config"

	"github.com/spf13/viper"
	"golang.org/x/sync/singleflight"
)

var (
	G_VP                  *viper.Viper
	G_Concurrency_Control = &singleflight.Group{}

	G_CONFIG config.AllConfig
)

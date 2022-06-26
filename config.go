package datamanager

import (
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

var Conf *appConfig

type appConfig struct {
	System struct {
		Port   int    `mapstructure:"port" yaml:"port"`
		Domain string `mapstructure:"domain" yaml:"domain"`
	} `mapstructure:"system" yaml:"system"`
	DB struct {
		UserName string `mapstructure:"username" yaml:"username"`
		Password string `mapstructure:"password" yaml:"password"`
		Host     string `mapstructure:"host" yaml:"host"`
		DB       string `mapstructure:"db" yaml:"db"`
	} `mapstructure:"db" yaml:"db"`
	Mongo struct {
		UserName string `mapstructure:"username" yaml:"username"`
		Password string `mapstructure:"password" yaml:"password"`
		Host     string `mapstructure:"host" yaml:"host"`
		DB       string `mapstructure:"db" yaml:"db"`
	} `mapstructure:"mongo" yaml:"mongo"`
}

// conf: yaml
func LoadConfig(conf string) error {
	pathExist := func(path string) bool {
		_, err := os.Stat(path)
		return !os.IsNotExist(err)
	}

	if !pathExist(conf) {
		return errors.New("文件不存在")
	}

	// load config
	v := viper.New()
	v.SetConfigFile(conf)
	v.SetConfigType(path.Ext(conf))
	if err := v.ReadInConfig(); err != nil {
		return err
	}
	v.WatchConfig()
	v.OnConfigChange(func(e fsnotify.Event) {
		fmt.Println("config file changed:", e.Name)
		if err := v.Unmarshal(&Conf); err != nil {
			fmt.Println(err)
		}
	})

	return v.Unmarshal(&Conf)
}

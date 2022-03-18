package core

import (
	"flag"
	"fmt"
	"os"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"github.com/wwqdrh/datamanager/internal/datautil"
)

const confStr = `
data-log:
  out-date: 10
  min-log-num: 10
  log-table-name: "_action_log"
  per-read-num: 1000
leveldb:
  log-path: "./version"
`

func InitConfig(path ...string) CoreOption {
	return func() error {
		config := getPath(path...)
		flag := true // 是否开启监听
		if config == "" {
			// 临时文件
			f := new(datautil.TempFile)
			f.NewFile(confStr)
			defer f.Close()
			config = f.TmpName
			flag = false
		}

		v := viper.New()
		v.SetConfigFile(config)
		v.SetConfigType("yaml")
		err := v.ReadInConfig()
		if err != nil {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}

		if flag {
			v.WatchConfig()
			v.OnConfigChange(func(e fsnotify.Event) {
				fmt.Println("config file changed:", e.Name)
				if err := v.Unmarshal(&G_CONFIG); err != nil {
					fmt.Println(err)
				}
			})
		}

		if err := v.Unmarshal(&G_CONFIG); err != nil {
			fmt.Println(err)
		}

		return nil
	}
}

// 获取配置文件的路径
func getPath(path ...string) string {
	var config string

	// 参数
	if len(path) == 1 && pathExist(path[0]) {
		return path[0]
	}

	// 命令行
	flag.StringVar(&config, "c", "", "choose config file.")
	flag.Parse()
	if config != "" && pathExist(config) {
		return config
	}

	// 环境变量
	config = os.Getenv(ConfigEnv)
	if config != "" && pathExist(config) {
		return config
	}

	return ""
}

func pathExist(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

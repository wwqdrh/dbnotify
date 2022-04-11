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
  stream-policy: "trigger"
  export-policy: "leveldb"
leveldb:
  log-path: "./version"
`

type AllConfig struct {
	DataLog struct {
		PerReadNum   int    `mapstructure:"per-read-num" yaml:"per-read-num"`     // 每次读取所需要的记录数
		OutDate      int    `mapstructure:"out-date" yaml:"out-date"`             // 过期时间 单位天
		MinLogNum    int    `mapstructure:"min-log-num" yaml:"min-log-num"`       // 最少保留条数
		LogTableName string `mapstructure:"log-table-name" yaml:"log-table-name"` // 日志临时表的名字
		StreamPolicy string `mapstructure:"stream-policy" yaml:"stream-policy"`   // 读取日志的策略
		ExportPolicy string `mapstructure:"export-policy" yaml:"export-policy"`   // 转存日志的策略
	} `mapstructure:"data-log" yaml:"data-log"`
	PostgresConf struct{} `mapstructure:"postgres" yaml:"postgres"`
	LevelDBConf  struct {
		LogPath string `mapstructure:"log-path" yaml:"log-path"`
	} `mapstructure:"leveldb" yaml:"leveldb"`
}

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

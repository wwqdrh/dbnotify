package core

import (
	"datamanager/server/common/datautil"
	"datamanager/server/global"
	"flag"
	"fmt"
	"os"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

const confStr = `
data-log:
  out-date: 10
  min-log-num: 10
  log-table-name: "policy"
  per-read-num: 1000
leveldb:
  log-path: "./version"
`

func InitConfig(path ...string) CoreOption {
	return func() error {
		var config string
		if len(path) == 0 {
			flag.StringVar(&config, "c", "", "choose config file.")
			flag.Parse()
			if config == "" { // 优先级: 命令行 > 环境变量 > 默认值
				if global.ConfigTemp {
					// 使用字符串 减少配置文件依赖
					f := new(datautil.TempFile)
					f.NewFile(confStr)
					defer f.Close()
					config = f.TmpName
					fmt.Printf("您正在使用编码中配置临时文件作为配置,config的路径为%v\n", config)
				} else if configEnv := os.Getenv(global.ConfigEnv); configEnv == "" {
					config = global.ConfigFile
					fmt.Printf("您正在使用config的默认值,config的路径为%v\n", global.ConfigFile)
				} else {
					config = configEnv
					fmt.Printf("您正在使用GVA_CONFIG环境变量,config的路径为%v\n", config)
				}
			} else {
				fmt.Printf("您正在使用命令行的-c参数传递的值,config的路径为%v\n", config)
			}
		} else {
			config = path[0]
			fmt.Printf("您正在使用func Viper()传递的值,config的路径为%v\n", config)
		}

		v := viper.New()
		v.SetConfigFile(config)
		v.SetConfigType("yaml")
		err := v.ReadInConfig()
		if err != nil {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}
		v.WatchConfig()

		v.OnConfigChange(func(e fsnotify.Event) {
			fmt.Println("config file changed:", e.Name)
			if err := v.Unmarshal(&global.G_CONFIG); err != nil {
				fmt.Println(err)
			}
		})
		if err := v.Unmarshal(&global.G_CONFIG); err != nil {
			fmt.Println(err)
		}
		global.G_VP = v

		return nil
	}
}

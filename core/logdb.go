package core

import (
	"os"

	"github.com/wwqdrh/datamanager/common/driver"
	"github.com/wwqdrh/datamanager/global"
)

// 初始化日志存储记录库
func InitLogDB() CoreOption {
	return func() error {
		logPath := global.G_CONFIG.LevelDBConf.LogPath
		if err := os.MkdirAll(logPath, 0777); err != nil {
			return err
		} // TODO: 需要显示传递文件路径否则重启日志不存在
		driver, err := driver.NewLevelDBDriver(logPath)
		if err != nil {
			return err
		}
		global.G_LOGDB = driver
		return nil
	}
}

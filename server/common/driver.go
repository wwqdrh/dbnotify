package common

import (
	"os"
	"path"

	"datamanager/server/pkg/plugger"

	"gorm.io/gorm"
)

var (
	LevelDBDriver  *plugger.LevelDBDriver
	PostgresDriver *plugger.PostgresDriver
)

func InitDriver(targetDB *gorm.DB, logPath string) (errs []error) {
	logPath = path.Join(logPath, "./version")
	if err := os.MkdirAll(logPath, 0777); err != nil {
		errs = append(errs, err)
		return
	} // TODO: 需要显示传递文件路径否则重启日志不存在
	// 目标库 postgres
	if err := InitPostgresDriver(targetDB); err != nil {
		errs = append(errs, err)
	}
	// 日志存储库
	if err := InitLevelDBDriver(logPath); err != nil {
		errs = append(errs, err)
	}
	return
}

func InitLevelDBDriver(logPath string) (e error) {
	driver, err := plugger.NewLevelDBDriver(logPath)
	if err != nil {
		e = err
	} else {
		LevelDBDriver = driver
	}
	return
}

// 初始化driver
func InitPostgresDriver(db *gorm.DB) (e error) {
	PostgresDriver = new(plugger.PostgresDriver).InitWithDB(db)
	return
}

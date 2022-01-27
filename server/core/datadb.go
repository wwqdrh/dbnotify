package core

import (
	"datamanager/server/common/driver"
	"datamanager/server/global"

	"gorm.io/gorm"
)

// InitDataDB 初始化postgres数据库
func InitDataDB(db ...*gorm.DB) CoreOption {
	return func() error {
		if len(db) == 0 {
			// 自己实例化一个
		}
		global.G_DATADB = new(driver.PostgresDriver).InitWithDB(db[0])
		return nil
	}
}

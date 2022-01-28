package core

import (
	"datamanager/server/common/driver"
	"datamanager/server/global"
	"errors"

	"gorm.io/gorm"
)

// InitDataDB 初始化postgres数据库
func InitDataDB(db ...*gorm.DB) CoreOption {
	return func() error {
		if len(db) == 0 || db[0] == nil {
			// 自己实例化一个
			return errors.New("暂时必须传递gorm.DB对象")
		}
		global.G_DATADB = new(driver.PostgresDriver).InitWithDB(db[0])
		return nil
	}
}

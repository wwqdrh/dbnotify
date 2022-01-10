package model

import (
	"time"

	"gorm.io/gorm"
)

type Version struct {
	ID       int       `gorm:"PRIMARY_KEY;AUTO_INCREMENT"`
	ActStr   string    // 存储json格式字符串 读取出来需要格式就知道是执行了insert 还是 update 然后修改了什么字段之类的
	CreateAt time.Time // 创建记录的时间 过期策略就是当低于某个时间之后这些数据都会进行删除
}

// 构造表结构
func (v *Version) Migrate(db *gorm.DB) {
	db.AutoMigrate(v)
}

func (v *Version) MigrateWithName(db *gorm.DB, tableName string) {
	db.Table(tableName).AutoMigrate(v)
}

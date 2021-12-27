package initial

import (
	"strings"
	"time"

	"gorm.io/gorm"
)

// 定义表的字段

// Policy 模型配置表 所有的数据表进行共用
type Policy struct {
	ID        int    `gorm:"PRIMARY_KEY;AUTO_INCREMENT"`
	TableName string `gorm:"index"` // 数据表的名字
	Fields    string // 需要进行触发的数据 a,b,c,d的格式
	Outdate   time.Time
}

// Version 数据版本 存储正向的操作以及逆向的 数据表是各用各的
type Version struct {
	ID         int    `gorm:"PRIMARY_KEY;AUTO_INCREMENT"`
	VersionID  int    `gorm:"index"` // 需要是全局id 不同的表之间可能会存在相同的版本id，回退是需要一起回退的
	RawSQL     string // 原始sql语句
	ReverseSQL string // 逆向操作的sql语句
	CreateAt   int    // 创建记录的时间 过期策略就是当低于某个时间之后这些数据都会进行删除
}

////////////////////
// repository
////////////////////

// MigrateInitial 初始化表结构
func MigrateInitial(db *gorm.DB) {
	MigratePolicy(db)
}

// MigratePolicy 创建policy表
func MigratePolicy(db *gorm.DB) {
	db.AutoMigrate(&Policy{})
}

// 创建数据库的version表
func MigrateVersion(db *gorm.DB, tableName string) {
	db.Table(tableName + "_version").AutoMigrate(&Version{})
}

func MigrateNewTablePolicy(db *gorm.DB, tableName string, fields []string) error {
	var policy Policy
	if err := db.Select("id").Where("table_name = ?", tableName).Find(&policy).Error; err == gorm.ErrRecordNotFound {
		if err := db.Create(&Policy{TableName: tableName, Outdate: time.Now(), Fields: strings.Join(fields, ",")}).Error; err != nil {
			return err
		}
	}
	MigrateVersion(db, tableName)
	return nil
}

// FindAllTablePolicy 读取表的所有的缓存策略
func FindAllTablePolicy(db *gorm.DB) []*Policy {
	var data []*Policy

	db.Select("table_name", "fields").Find(&data)

	return data
}

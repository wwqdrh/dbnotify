package model

import (
	"strings"

	"gorm.io/gorm"
)

// policyModel 模型的表结构
type Policy struct {
	ID            int    `gorm:"PRIMARY_KEY;AUTO_INCREMENT"`
	TableName     string `gorm:"index"` // 数据表的名字
	PrimaryFields string `gorm:"primary_fields"`
	Fields        string // 需要进行触发的数据 a,b,c,d的格式
	MinLogNum     int
	Outdate       int // 单位天数 默认为1个月
}

// 构造表结构
func (p Policy) Migrate(db *gorm.DB) {
	db.AutoMigrate(p)
}

func (p Policy) MigrateWithName(db *gorm.DB, tableName string) {
	db.Table(tableName).AutoMigrate(p)
}

func (p Policy) GetAllData(db *gorm.DB) []*Policy {
	res := []*Policy{}
	db.Find(&res)
	return res
}

// CreateNoExist 如果记录不存在那么就新建 否则就返回(需要把对应的id给返回出来)
func (p *Policy) CreateNoExist(db *gorm.DB, model *Policy, condition map[string]interface{}) error {
	condStr := []string{}
	values := []interface{}{}
	for cond, val := range condition {
		condStr = append(condStr, cond+" = ? ")
		values = append(values, val)
	}

	if err := db.Where(strings.Join(condStr, "and"), values...).First(&p).Error; err == gorm.ErrRecordNotFound {
		if err := db.Create(model).Error; err != nil {
			return err
		}
	}

	return nil
}

// DeleteByTableName 根据表名删除记录
func (p *Policy) DeleteByTableName(db *gorm.DB, tableName string) error {
	return db.Where("table_name = ?", tableName).Delete(&Policy{}).Error
}

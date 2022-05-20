package postgres

import (
	"errors"

	"gorm.io/gorm"
)

var (
	db         *gorm.DB
	PolicyName string = "policy" // 自定义的表名
)

type Policy struct {
	ID        int    `gorm:"primaryKey;autoIncreatment"`
	TableName string `gorm:"column:table_name;uniqueIndex;type:varchar(20);not null"` // 数据表的名字
	MinLogNum int    `gorm:"column:min_lognum"`                                       // 与outdate一起使用的，即使过期了也要保留最少的记录条数
	Outdate   int    `gorm:"column:out_date"`                                         // 单位天数 默认为1个月
	Relations string `gorm:"column:relations;type:varchar(100)"`                      // `gorm:"description:"[表名].[字段名];[表名].[字段名]...""` 方便多表关联记录的查询
}

func (p *Policy) Validate() error {
	if p.TableName == "" {
		return errors.New("table_name不能为空")
	}
	if p.MinLogNum < 0 {
		p.MinLogNum = 10
	}
	if p.Outdate < 0 {
		p.Outdate = 7
	}
	return nil
}

// migrate with custom tablename
func (p Policy) Migrate() error {
	return db.Table(PolicyName).AutoMigrate(p)
}

// save policy
func (p *Policy) Save() (*Policy, error) {
	if err := p.Validate(); err != nil {
		return p, err
	}

	if err := db.Table(PolicyName).Create(p).Error; err != nil {
		return nil, err
	} else {
		return p, nil
	}
}

// get all policy
func (p Policy) GetAllData(db *gorm.DB) []*Policy {
	res := []*Policy{}
	db.Find(&res)
	return res
}

// DeleteByTableName 根据表名删除记录
func (p *Policy) DeleteByTableName(tableName string) error {
	return db.Table(PolicyName).Where("table_name = ?", tableName).Delete(&Policy{}).Error
}

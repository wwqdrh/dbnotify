package policytable

import (
	"strings"

	"gorm.io/gorm"

	"github.com/wwqdrh/datamanager/internal/driver"
	"github.com/wwqdrh/datamanager/internal/pgwatcher/base"
)

// policyModel 模型的表结构
type Policy struct {
	ID            int    `gorm:"PRIMARY_KEY;AUTO_INCREMENT"`
	TableName     string `gorm:"index"` // 数据表的名字
	PrimaryFields string `gorm:"primary_fields"`
	Fields        string // 需要进行触发的数据 a,b,c,d的格式
	MinLogNum     int
	Outdate       int // 单位天数 默认为1个月
	RelaField     string
	Relations     string // `gorm:"description:"[表名].[字段名];[表名].[字段名]...""`
}

// buildPolicy 为新表注册策略
func NewPolicy(db *driver.PostgresDriver, tableName string, min_log_num, outdate int, pol *base.TablePolicy, handler base.IStructHandler) (*Policy, error) {
	// 最少记录条数
	if min_log_num < pol.MinLogNum {
		min_log_num = pol.MinLogNum
	}
	if outdate < pol.Outdate {
		outdate = pol.Outdate
	}
	primary, err := db.GetPrimary(tableName)
	if err != nil {
		return nil, err
	}
	primaryFields := strings.Join(primary, ",")
	fieldsStr := []string{}
	allFields := handler.GetFields(tableName)
	ignoreMapping := map[string]bool{}
	for _, item := range pol.IgnoreFields {
		ignoreMapping[item] = true
	}
	if len(pol.SenseFields) == 0 || pol.SenseFields[0] == "" {
		for _, item := range allFields {
			if _, ok := ignoreMapping[item.FieldID]; !ok {
				fieldsStr = append(fieldsStr, item.FieldID)
			}
		}
	} else {
		for _, item := range pol.SenseFields {
			if _, ok := ignoreMapping[item]; !ok {
				fieldsStr = append(fieldsStr, item)
			}
		}
	}
	policy := &Policy{
		TableName:     tableName,
		PrimaryFields: primaryFields,
		Fields:        strings.Join(fieldsStr, ","),
		MinLogNum:     min_log_num,
		Outdate:       outdate,
		RelaField:     pol.RelaField,
		Relations:     pol.Relations,
	}

	if err := policy.CreateNoExist(
		db.DB, policy,
		map[string]interface{}{"table_name": tableName},
	); err != nil {
		return nil, err
	}
	return policy, nil
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

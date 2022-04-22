package policy

import (
	"context"
	"strings"
	"time"

	"github.com/wwqdrh/datamanager/internal/driver"
	"gorm.io/gorm"
)

type TablePolicy struct {
	Table        interface{}
	MinLogNum    int
	Outdate      int
	RelaField    string
	Relations    string
	SenseFields  []string
	IgnoreFields []string
}

type (
	IStructHandler interface {
		GetTables() []*Table
		GetFields(string) []*Fields
		GetFieldName(string, string) string // 通过表名，字段id 获取字段名
		GetTableName(string) string         // 通过表id获取表名字
	}

	Table struct {
		TableID   string `json:"table_id"`
		TableName string `json:"table_name"`
		IsListen  bool   `json:"is_listen"`
	}

	Fields struct {
		FieldID   string `json:"field_id"`
		FieldName string `json:"field_name"`
	}
)

type ITriggerStream interface {
	IStreamInitial
	IStreamPolicy
}

// stream的初始化接口
type IStreamInitial interface {
	Install(...TablePolicy) (chan map[string]interface{}, error)           // 执行初始化问题, 返回通道用于exporter来获取数据并使用
	Start(ctx context.Context, tableName string, id uint64, num int) error // 开启读取日志并发送消息
}

// stream的策略接口
type IStreamPolicy interface {
	GetAllPolicy() map[string]*Policy                      // 获取所有的表格当前策略
	AddPolicy(TablePolicy) error                           // 添加新的表策略
	ModifyPolicy(...interface{}) error                     // 修改配置
	RemovePolicy(string) error                             // 删除对应表名的策略
	ListTableField(tableName string) []*Fields             // 返回表的字段列表
	ModifyOutdate(tableName string, outdate int) error     // 修改表格的过期时间
	ModifyField(tableName string, fields []string) error   // 修改监听字段
	ModifyMinLogNum(tableName string, minLogNum int) error // 修改最小日志数
}

////////////////////
// 模型定义
////////////////////

type (
	LogTable struct {
		ID        uint64      `gorm:"PRIMARY_KEY"`
		TableName string      `gorm:"table_name"`
		Log       driver.JSON `gorm:"TYPE:json"` // ddl: []string; dml: ...
		Action    string
		Time      *time.Time
	}
)

// 构造表结构
func (p LogTable) Migrate(db *gorm.DB) {
	db.AutoMigrate(p)
}

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

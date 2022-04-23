package policytable

import (
	"time"

	"github.com/wwqdrh/datamanager/internal/driver"
	"gorm.io/gorm"
)

type LogTable struct {
	ID        uint64      `gorm:"PRIMARY_KEY"`
	TableName string      `gorm:"table_name"`
	Log       driver.JSON `gorm:"TYPE:json"` // ddl: []string; dml: ...
	Action    string
	Time      *time.Time
}

// 构造表结构
func (p LogTable) Migrate(db *gorm.DB) {
	db.AutoMigrate(p)
}

// ReadAndDelete
func (p LogTable) ReadAndDeleteByID(db *gorm.DB, logTableName string, tableName string, num int) ([]*LogTable, error) {
	var res []*LogTable
	if err := db.Table(logTableName).Where("table_name = ?", tableName).Where("id > ?", 0).Limit(num).Find(&res).Error; err != nil {
		return nil, err
	}

	ids := []uint64{}
	for _, item := range res {
		ids = append(ids, item.ID)
	}
	if len(ids) > 0 {
		if err := db.Table(logTableName).Delete(&LogTable{}, ids).Error; err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (p LogTable) ReadBytableNameAndLimit(db *gorm.DB, logTableName string, tableName string, id uint64, num int) ([]*LogTable, error) {
	var res []*LogTable
	if err := db.Table(logTableName).Where("table_name = ?", tableName).Where("id > ?", id).Limit(num).Find(&res).Error; err != nil {
		return nil, err
	}
	return res, nil
}

func (p LogTable) DeleteByID(db *gorm.DB, logTableName string, data []*LogTable) error {
	if len(data) == 0 {
		return nil
	}
	ids := []uint64{}
	for _, item := range data {
		ids = append(ids, item.ID)
	}
	return db.Table(logTableName).Delete(&LogTable{}, ids).Error
}

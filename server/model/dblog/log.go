package dblog

import (
	"time"

	"datamanager/server/pkg/plugger"

	"gorm.io/gorm"
)

type (
	LogTable struct {
		ID        uint64       `gorm:"PRIMARY_KEY"`
		TableName string       `gorm:"table_name"`
		Log       plugger.JSON `gorm:"TYPE:json"`
		Action    string
		Time      *time.Time
	}
	LogTableRepository struct {
		db           *gorm.DB
		logTableName string
	}
)

// 构造表结构
func (p LogTable) Migrate(db *gorm.DB) {
	db.AutoMigrate(p)
}

// ReadAndDelete
func (r LogTableRepository) ReadAndDeleteByID(tableName string, num int) ([]*LogTable, error) {
	var res []*LogTable
	if err := r.db.Table(r.logTableName).Where("table_name = ?", tableName).Where("id > ?", 0).Limit(num).Find(&res).Error; err != nil {
		return nil, err
	}

	ids := []uint64{}
	for _, item := range res {
		ids = append(ids, item.ID)
	}
	if len(ids) > 0 {
		if err := r.db.Table(r.logTableName).Delete(&LogTable{}, ids).Error; err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (r LogTableRepository) ReadBytableNameAndLimit(tableName string, id uint64, num int) ([]*LogTable, error) {
	var res []*LogTable
	if err := r.db.Table(r.logTableName).Where("table_name = ?", tableName).Where("id > ?", id).Limit(num).Find(&res).Error; err != nil {
		return nil, err
	}
	return res, nil
}

func (r LogTableRepository) DeleteByID(data []*LogTable) error {
	if len(data) == 0 {
		return nil
	}
	ids := []uint64{}
	for _, item := range data {
		ids = append(ids, item.ID)
	}
	return r.db.Table(r.logTableName).Delete(&LogTable{}, ids).Error
}

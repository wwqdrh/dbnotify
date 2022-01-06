package model

import (
	"datamanager/pkg/plugger/postgres"
	"time"

	"gorm.io/gorm"
)

type (
	LogTable struct {
		ID        int           `gorm:"PRIMARY_KEY;AUTO_INCREMENT"`
		TableName string        `gorm:"table_name"`
		Log       postgres.JSON `gorm:"TYPE:json"`
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

	ids := []int{}
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

func (r LogTableRepository) ReadBytableNameAndLimit(tableName string, id, num int) ([]*LogTable, error) {
	var res []*LogTable
	if err := r.db.Table(r.logTableName).Where("table_name = ?", tableName).Where("id > ?", id).Limit(num).Find(&res).Error; err != nil {
		return nil, err
	}
	return res, nil
}

func (r LogTableRepository) DeleteByID(data []*LogTable) error {
	ids := []int{}
	for _, item := range data {
		ids = append(ids, item.ID)
	}
	return r.db.Table(r.logTableName).Delete(&LogTable{}, ids).Error
}

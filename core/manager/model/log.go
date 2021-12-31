package model

import (
	"datamanager/pkg/plugger/postgres"
	"time"

	"gorm.io/gorm"
)

type LogTable struct {
	ID        int           `gorm:"PRIMARY_KEY;AUTO_INCREMENT"`
	TableName string        `gorm:"table_name"`
	Log       postgres.JSON `gorm:"TYPE:json"`
	Action    string
	Time      *time.Time
}

// 构造表结构
func (p LogTable) Migrate(db *gorm.DB) {
	db.AutoMigrate(p)
}

// ReadAndDelete
func (LogTable) ReadAndDeleteByID(db *gorm.DB, logTableName, tableName string, num int) ([]*LogTable, error) {
	var res []*LogTable
	if err := db.Table(logTableName).Where("table_name = ?", tableName).Where("id > ?", 0).Limit(num).Find(&res).Error; err != nil {
		return nil, err
	}

	ids := []int{}
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

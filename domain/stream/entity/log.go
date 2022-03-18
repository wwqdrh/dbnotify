package entity

import (
	"time"

	"github.com/wwqdrh/datamanager/internal/driver"

	"gorm.io/gorm"
)

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

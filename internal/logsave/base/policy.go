package base

import (
	"time"

	"github.com/wwqdrh/datamanager/internal/driver"
)

type Policy struct {
	TableName     string // 数据表的名字
	PrimaryFields string
	Fields        string // 需要进行触发的数据 a,b,c,d的格式
	MinLogNum     int
	Outdate       int // 单位天数 默认为1个月
	RelaField     string
	Relations     string // `gorm:"description:"[表名].[字段名];[表名].[字段名]...""`
}

type LogTable struct {
	TableName string
	Log       driver.JSON
	Action    string
	Time      time.Time
}

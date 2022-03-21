package base

import (
	"context"
	"time"
)

// exporter相关

type IExpoterInitial interface {
	Install(context.Context, chan map[string]interface{}, IExporterAlias) error // 装载，chan string，接收stream生成的log
	Start() error
}

type IExporterLog interface {
	// 获取所有的日志
	GetAllLogger(tableName string, primaryFields []string, start, end *time.Time, page, pageSize int) ([]map[string]interface{}, error)
	// 根据主键、开始结束时间、页数，每页个数
	GetLoggerByTime(tableName string, primaryID string, startTime, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error)
}

type IExporterAlias interface {
	TransPrimary(tableName string, primaryStr string) string                                // 主字段之间的转换
	TransField(tableName string, records []map[string]interface{}) []map[string]interface{} // 字段之间的转换
}

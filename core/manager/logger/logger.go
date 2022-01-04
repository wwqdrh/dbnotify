package logger

import (
	"datamanager/core/manager/model"
	"time"
)

type ILogger interface {
	// Initial()       // logger的初始化 比如注册触发器之类的
	// Compack() error // 执行备份的逻辑 就是说针对table如何进行备份，这里是采用触发器存储在额外的表中
	Dump(data []*model.LogTable, primaryFields []string, fields ...string) ([]map[string]interface{}, error)                          // 将log struct转换成map形式
	SetSenseFields(tableName string, data []string)                                                                                   // 设置关心的字段，只有针对这些字段才会触发转换
	GetLogger(tableName string, fields []string, startTime, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) // 读取logger
	Run(tableName string, fields []string, outdate int)                                                                               // 日志处理逻辑启动
}

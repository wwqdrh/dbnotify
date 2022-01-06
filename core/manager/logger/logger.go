package logger

import (
	"datamanager/core/manager/model"
	"time"
)

type ILogger interface {
	// Initial()       // logger的初始化 比如注册触发器之类的
	// Compack() error // 执行备份的逻辑 就是说针对table如何进行备份，这里是采用触发器存储在额外的表中
	Dump([]*model.LogTable, []string, ...string) ([]map[string]interface{}, error)                  // 将log struct转换成map形式
	SetSenseFields(string, []string)                                                                // 设置关心的字段，只有针对这些字段才会触发转换
	GetLogger(string, []string, *time.Time, *time.Time, int, int) ([]map[string]interface{}, error) // 读取logger
	Run(string, []string, int) error
	// 将run拆分
	Load() func(tableName string, id, num int) ([]*model.LogTable, error) // 从源数据读取
	Store() func(policy *model.Policy, data []*model.LogTable) error      // 写入到日志库中                                                                          // 日志处理逻辑启动
}

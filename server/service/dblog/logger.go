package dblog

import (
	"time"

	dblog_model "datamanager/server/model/dblog"
)

type ILogger interface {
	// Initial()       // logger的初始化 比如注册触发器之类的
	// Compack() error // 执行备份的逻辑 就是说针对table如何进行备份，这里是采用触发器存储在额外的表中
	Dump([]*dblog_model.LogTable, []string) ([]map[string]interface{}, error)                     // 将log struct转换成map形式
	SetSenseFields(string, []string)                                                              // 设置关心的字段，只有针对这些字段才会触发转换
	GetLogger(string, string, *time.Time, *time.Time, int, int) ([]map[string]interface{}, error) // 读取logger
	Run(string, []string, int, int) error
	GetAllLogger(tableName string, primaryFields []string, start, end *time.Time, page, pageSize int) ([]map[string]interface{}, error) // 获取整体的记录行的信息 只包含主键
	// 将run拆分
	Load() func(tableName string, id uint64, num int) ([]*dblog_model.LogTable, error) // 从源数据读取
	Store() func(policy *dblog_model.Policy, data []*dblog_model.LogTable) error       // 写入到日志库中                                                                          // 日志处理逻辑启动
}

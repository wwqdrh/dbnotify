package dblog

import (
	"strings"
	"sync"

	dblog_model "datamanager/server/model/dblog"
	"datamanager/server/types"
)

// 将生产者与消费者分开处理的logger
type (
	ConsumerLogger struct {
		DefaultLogger
	}
)

func NewConsumerLogger(logTableName string, handler types.IStructHandler, getPolicy func(string) interface{}) ILogger {
	logger := &ConsumerLogger{
		DefaultLogger: DefaultLogger{
			senseFields:  map[string][]string{},
			task:         &sync.Map{},
			logTableName: logTableName,
			getPolicy:    getPolicy,
			handler:      handler,
		},
	}
	return logger
}

// 有种情况是刚注册未设置监听字段
func (l *ConsumerLogger) Load() func(tableName string, id uint64, num int) ([]*dblog_model.LogTable, error) {
	return func(tableName string, id uint64, num int) ([]*dblog_model.LogTable, error) {
		data, err := dblog_model.LogTableRepo.ReadBytableNameAndLimit(tableName, id, num)
		if err != nil {
			return nil, err
		}
		return data, nil
	}
}

func (l *ConsumerLogger) Store() func(policy *dblog_model.Policy, data []*dblog_model.LogTable) error {
	return func(policy *dblog_model.Policy, data []*dblog_model.LogTable) error {
		primaryFields := strings.Split(policy.PrimaryFields, ",")
		datar, err := l.Dump(data, primaryFields)
		if err != nil {
			return err
		}
		if datar == nil {
			return nil // 监听字段未命中
		}

		if err = dblog_model.LogRepoV2.Write(policy.TableName, datar, policy.Outdate, policy.MinLogNum); err != nil {
			return err
		}
		return nil
	}
}

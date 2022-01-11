package logger

import (
	"errors"
	"strings"
	"sync"

	"datamanager/manager/model"
	"datamanager/pkg/plugger"
)

// 将生产者与消费者分开处理的logger
type (
	ConsumerLogger struct {
		DefaultLogger
	}
)

func NewConsumerLogger(sourceDB *plugger.PostgresDriver, targetDB *plugger.LevelDBDriver, logTableName string, getPolicy func(string) interface{}) ILogger {
	logger := &ConsumerLogger{
		DefaultLogger: DefaultLogger{
			sourceDb:     sourceDB,
			targetDb:     targetDB,
			senseFields:  map[string][]string{},
			task:         &sync.Map{},
			logTableName: logTableName,
			getPolicy:    getPolicy,
		},
	}
	return logger
}

// 有种情况是刚注册未设置监听字段
func (l *ConsumerLogger) Load() func(tableName string, id uint64, num int) ([]*model.LogTable, error) {
	return func(tableName string, id uint64, num int) ([]*model.LogTable, error) {
		policy := (l.getPolicy(tableName)).(*model.Policy)
		senseFields := strings.Split(policy.Fields, ",")
		if len(senseFields) == 0 || senseFields[0] == "" {
			return nil, errors.New("未设置监听字段") // 未设置监听字段
		}
		data, err := model.LogTableRepo.ReadBytableNameAndLimit(tableName, id, num)
		if err != nil {
			return nil, err
		}
		return data, nil
	}
}

func (l *ConsumerLogger) Store() func(policy *model.Policy, data []*model.LogTable) error {
	return func(policy *model.Policy, data []*model.LogTable) error {
		primaryFields := strings.Split(policy.PrimaryFields, ",")
		senseFields := strings.Split(policy.Fields, ",")
		datar, err := l.Dump(data, primaryFields, senseFields...)
		if err != nil {
			return err
		}
		if datar == nil {
			return nil // 监听字段未命中
		}

		// if err = model.LogLocalLogRepo.Write(policy.TableName, datar, senseFields, policy.Outdate, policy.MinLogNum); err != nil {
		// 	return err
		// }
		if err = model.LogRepoV2.Write(policy.TableName, datar, policy.Outdate, policy.MinLogNum); err != nil {
			return err
		}
		return nil
	}
}

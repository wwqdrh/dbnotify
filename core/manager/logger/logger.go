package logger

import (
	"datamanager/core/manager/model"
	"datamanager/pkg/datautil"
	"datamanager/pkg/plugger/leveldb"
	"datamanager/pkg/plugger/postgres"
	"time"
)

// 日志格式转换

type (
	ILogger interface {
		Dump(data []*model.LogTable, fields ...string) ([]map[string]interface{}, error)
		SetSenseFields(tableName string, data []string)
		Run(tableName string, fields []string, outdate int) // 日志处理逻辑启动
		GetLogger(tableName string, fields []string, startTime, endTime *time.Time) (map[string]interface{}, error)
	}

	DefaultLogger struct {
		senseFields  map[string][]string //
		sourceDb     *postgres.PostgresDriver
		task         map[string]bool // 当前任务的存储
		targetDb     *leveldb.LevelDBDriver
		logTableName string
	}
)

func NewDefaultLogger(sourceDB *postgres.PostgresDriver, targetDB *leveldb.LevelDBDriver, logTableName string) ILogger {
	return &DefaultLogger{
		sourceDb:     sourceDB,
		targetDb:     targetDB,
		senseFields:  map[string][]string{},
		task:         map[string]bool{},
		logTableName: logTableName,
	}
}

func (l *DefaultLogger) SetSenseFields(tableName string, data []string) {
	l.senseFields[tableName] = data
}

// {
// 	"log": {
// 		"data": [
// 			{
// 				"address": "das                                               ",
// 				"age": 18,
// 				"id": 20,
// 				"name": "hui",
// 				"salary": 1500
// 			}
// 		]
// 	},
// 	"time": "2021-12-30T11:50:52.233783+08:00",
// 	"action": "delete"
// }
func (l DefaultLogger) Dump(logData []*model.LogTable, fields ...string) ([]map[string]interface{}, error) {
	response := make([]map[string]interface{}, 0, len(logData))

	for _, item := range logData {
		data, _ := datautil.JsonToMap(string(item.Log))
		log := l.wash(data, item.Action, l.senseFields[item.TableName]...)
		if log == nil {
			continue
		}
		cur := map[string]interface{}{
			"log":    log,
			"action": item.Action,
			"time":   item.Time,
		}
		response = append(response, cur)
	}
	return response, nil
}

// update的log中是before与after两个字段，delete与insert是一个data字段
func (l DefaultLogger) wash(data map[string]interface{}, action string, fieldNames ...string) map[string]interface{} {
	if len(fieldNames) == 0 {
		return data
	}

	switch action {
	case "update":
		fields := map[string]interface{}{}
		beforeDatas, ok := data["before"].([]interface{})
		if !ok {
			return nil
		}
		afterDatas, ok := data["after"].([]interface{})
		if !ok {
			return nil
		}

		for _, item := range fieldNames {
			beforeItem := beforeDatas[0].(map[string]interface{})
			afterItem := afterDatas[0].(map[string]interface{})

			beforeVal, _ := beforeItem[item]
			afterVal, _ := afterItem[item]
			if beforeVal != afterVal {
				fields[item] = map[string]interface{}{
					"before": beforeVal,
					"after":  afterVal,
				}
			}
		}
		if len(fields) == 0 {
			return nil
		}
		return fields
	case "insert":
		fields := map[string]interface{}{}

		oriDatas, ok := data["data"].([]interface{})
		if !ok {
			return nil
		}
		for _, item := range fieldNames {
			curItem := oriDatas[0].(map[string]interface{})
			if val, ok := curItem[item]; ok {
				fields[item] = val
			}
		}
		if len(fields) == 0 {
			return nil
		}
		return fields
	case "delete":
		fields := map[string]interface{}{}
		oriDatas, ok := data["data"].([]interface{})
		if !ok {
			return nil
		}
		for _, item := range fieldNames {
			curItem := oriDatas[0].(map[string]interface{})
			if val, ok := curItem[item]; ok {
				fields[item] = val
			}
		}
		if len(fields) == 0 {
			return nil
		}
		return fields
	default:
		return nil
	}
}

func (l DefaultLogger) Run(tableName string, fields []string, outdate int) {
	if _, ok := l.task[tableName]; ok {
		return
	}
	l.task[tableName] = true

	fn := func() error {
		// 执行读取表的逻辑 每次读200行
		data, err := model.LogTableRepo.ReadAndDeleteByID(l.sourceDb.DB, l.logTableName, tableName, 200)
		if err != nil {
			delete(l.task, tableName)
			return err
		}

		datar, err := l.Dump(data, fields...)
		if err != nil {
			delete(l.task, tableName)
			return err
		}

		if err = model.LogLocalLogRepo.Write(tableName, datar, fields, outdate); err != nil {
			delete(l.task, tableName)
			return err
		}

		return nil
	}

	go datautil.NewMyTick(time.Duration(10)*time.Minute, fn).Start()
}

func (l DefaultLogger) GetLogger(tableName string, fields []string, startTime, endTime *time.Time) (map[string]interface{}, error) {
	return model.LogLocalLogRepo.SearchRecord(tableName, fields, startTime, endTime)
}

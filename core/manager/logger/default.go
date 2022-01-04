package logger

import (
	"datamanager/core/manager/model"
	"datamanager/pkg/datautil"
	"datamanager/pkg/plugger/leveldb"
	"datamanager/pkg/plugger/postgres"
	"strings"
	"time"
)

// 日志格式转换

type (
	DefaultLogger struct {
		senseFields  map[string][]string //
		sourceDb     *postgres.PostgresDriver
		task         map[string]bool // 当前任务的存储
		targetDb     *leveldb.LevelDBDriver
		logTableName string
		getPolicy    func(string) interface{} // 根据tableName获取配置
	}
)

func NewDefaultLogger(sourceDB *postgres.PostgresDriver, targetDB *leveldb.LevelDBDriver, logTableName string, getPolicy func(string) interface{}) ILogger {
	return &DefaultLogger{
		sourceDb:     sourceDB,
		targetDb:     targetDB,
		senseFields:  map[string][]string{},
		task:         map[string]bool{},
		logTableName: logTableName,
		getPolicy:    getPolicy,
	}
}

func (l *DefaultLogger) SetSenseFields(tableName string, data []string) {
	l.senseFields[tableName] = data
}

func (l DefaultLogger) Dump(logData []*model.LogTable, primaryFields []string, fields ...string) ([]map[string]interface{}, error) {
	response := make([]map[string]interface{}, 0, len(logData))

	for _, item := range logData {
		data, _ := datautil.JsonToMap(string(item.Log))
		log := l.wash(data, item.Action, primaryFields, l.senseFields[item.TableName]...)
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
func (l DefaultLogger) wash(data map[string]interface{}, action string, primaryFields []string, fieldNames ...string) map[string]interface{} {
	if len(fieldNames) == 0 {
		return data
	}

	switch action {
	case "update":
		res := map[string]interface{}{} // data, primary
		fields := map[string]interface{}{}
		primaryData := map[string]interface{}{} // 主键的数据
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
		for _, item := range primaryFields {
			beforeItem := beforeDatas[0].(map[string]interface{})
			afterItem := afterDatas[0].(map[string]interface{})

			beforeVal, _ := beforeItem[item]
			afterVal, _ := afterItem[item]
			primaryData[item] = map[string]interface{}{
				"before": beforeVal,
				"after":  afterVal,
			}
		}
		if len(fields) == 0 {
			return nil
		}
		res["data"] = fields
		res["primary"] = primaryData
		return res
	case "insert":
		res := map[string]interface{}{} // data, primary
		fields := map[string]interface{}{}
		primaryData := map[string]interface{}{} // 主键的数据
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
		for _, item := range primaryFields {
			curItem := oriDatas[0].(map[string]interface{})
			if val, ok := curItem[item]; ok {
				primaryData[item] = val
			}
		}
		if len(fields) == 0 {
			return nil
		}
		res["data"] = fields
		res["primary"] = primaryData
		return res
	case "delete":
		res := map[string]interface{}{} // data, primary
		fields := map[string]interface{}{}
		primaryData := map[string]interface{}{} // 主键的数据
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
		for _, item := range primaryFields {
			curItem := oriDatas[0].(map[string]interface{})
			if val, ok := curItem[item]; ok {
				primaryData[item] = val
			}
		}
		if len(fields) == 0 {
			return nil
		}
		res["data"] = fields
		res["primary"] = primaryData
		return res
	default:
		return nil
	}
}

func (l DefaultLogger) Run(tableName string, fields []string, outdate int) {
	if _, ok := l.task[tableName]; ok {
		return
	}
	policy := (l.getPolicy(tableName)).(*model.Policy)
	primaryFields := strings.Split(policy.PrimaryFields, ",")
	senseFields := strings.Split(policy.Fields, ",")

	if len(senseFields) == 0 || senseFields[0] == "" {
		return // 未设置监听字段
	}

	l.task[tableName] = true
	fn := func() error {
		// 执行读取表的逻辑 每次读200行
		data, err := model.LogTableRepo.ReadAndDeleteByID(l.sourceDb.DB, l.logTableName, tableName, 200)
		if err != nil {
			delete(l.task, tableName)
			return err
		}

		datar, err := l.Dump(data, primaryFields, fields...)
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

func (l DefaultLogger) GetLogger(tableName string, fields []string, startTime, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	return model.LogLocalLogRepo.SearchRecordByField(tableName, fields, startTime, endTime, page, pageSize)
}

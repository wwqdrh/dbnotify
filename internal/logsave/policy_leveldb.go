package logsave

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/wwqdrh/datamanager/internal/datautil"
	"github.com/wwqdrh/datamanager/internal/driver"
	"github.com/wwqdrh/datamanager/internal/logsave/base"
	"github.com/wwqdrh/datamanager/internal/logsave/model"
)

type LogSaveLeveldb struct {
	Queue *datautil.Queue
	LogDB *driver.LevelDBDriver
}

func (l *LogSaveLeveldb) Write(data map[string]interface{}) error {
	var logs []base.LogTable
	{
		b, err := json.Marshal(data["data"])
		if err != nil {
			return err
		}
		json.Unmarshal(b, &logs)
	}
	var policy base.Policy
	{
		b, err := json.Marshal(data["policy"])
		if err != nil {
			return err
		}
		json.Unmarshal(b, &policy)
	}

	// 开始写数据
	primaryFields := strings.Split(policy.PrimaryFields, ",")
	senseFields := strings.Split(policy.Fields, ",")
	datar, err := l.dump(logs, primaryFields, senseFields)
	if err != nil {
		return err
	}
	if datar == nil {
		return nil // 监听字段未命中
	}

	if err = new(model.LocalLog2).Write(l.LogDB, policy.TableName, primaryFields, datar, policy.Outdate, policy.MinLogNum); err != nil {
		return err
	}
	return nil
}

// Dump 将log struct转换成map形式
// 需要处理ddl的log格式以及dml数据的log格式
func (s *LogSaveLeveldb) dump(logData []base.LogTable, primaryFields []string, senseFields []string) ([]map[string]interface{}, error) {
	response := make([]map[string]interface{}, 0, len(logData))
	var val map[string]interface{}
	for _, item := range logData {
		switch item.Action {
		case "ddl":
			val = s.dumpDDL(&item)
		default:
			val = s.dumpDML(&item, primaryFields, senseFields)
		}
		if val != nil {
			response = append(response, val)
		}

	}
	return response, nil
}

// 处理ddl
func (s *LogSaveLeveldb) dumpDDL(item *base.LogTable) map[string]interface{} {
	data, _ := datautil.JsonToMap(string(item.Log))
	return map[string]interface{}{
		"log":    data["data"].([]interface{})[0].(string), // 一般只有第一个ddl语句
		"action": item.Action,
		"time":   datautil.ParseTime(&item.Time),
	}
}

// 处理dml
func (s *LogSaveLeveldb) dumpDML(item *base.LogTable, primaryFields []string, senseFields []string) map[string]interface{} {
	data, _ := datautil.JsonToMap(string(item.Log))
	log := s.wash(data, item.Action, primaryFields, senseFields...)
	if log == nil {
		return nil
	}
	return map[string]interface{}{
		"log":    log,
		"action": item.Action,
		"time":   datautil.ParseTime(&item.Time),
	}
}

// Wash 写入日志时对数据结构的一些处理
func (s *LogSaveLeveldb) wash(data map[string]interface{}, action string, primaryFields []string, fieldNames ...string) map[string]interface{} {
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
			if !reflect.DeepEqual(beforeVal, afterVal) {
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
		res["all"] = afterDatas[0]
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
		res["all"] = oriDatas[0]
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
		res["all"] = oriDatas[0]
		return res
	default:
		return nil
	}
}

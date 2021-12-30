package logger

import (
	"datamanager/core/manager/trigger"
	"datamanager/pkg/datautil"
)

// 日志格式转换

type (
	ILogger interface {
		Dump(data []*trigger.LogTable, fields ...string) (interface{}, error)
		SetSenseFields(data []string)
	}

	DefaultLogger struct {
		senseFields []string //
	}
)

func NewDefaultLogger() ILogger {
	return &DefaultLogger{}
}

func (l *DefaultLogger) SetSenseFields(data []string) {
	l.senseFields = data
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
func (l DefaultLogger) Dump(logData []*trigger.LogTable, fields ...string) (interface{}, error) {
	response := make([]map[string]interface{}, 0, len(logData))
	var senseFields []string
	if len(fields) == 0 {
		senseFields = l.senseFields
	} else {
		senseFields = fields
	}

	for _, item := range logData {
		data, _ := datautil.JsonToMap(string(item.Log))

		cur := map[string]interface{}{
			"log":    l.wash(data, item.Action, senseFields...),
			"action": item.Action,
			"time":   item.Time,
		}
		response = append(response, cur)
	}
	return response, nil
}

// update的log中是before与after两个字段，delete与insert是一个data字段
func (l DefaultLogger) wash(data map[string]interface{}, action string, fieldNames ...string) interface{} {
	if len(fieldNames) == 0 {
		return data
	}

	var res map[string]interface{}
	switch action {
	case "update":
		fields := map[string]interface{}{}
		res = map[string]interface{}{"data": fields}
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
		return res
	case "insert":
		fields := map[string]interface{}{}
		res = map[string]interface{}{"data": fields}

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
		return res
	case "delete":
		fields := map[string]interface{}{}
		res = map[string]interface{}{"data": fields}
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
		return res
	default:
		return nil
	}
}

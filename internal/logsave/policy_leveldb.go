package logsave

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/wwqdrh/datamanager/internal/datautil"
	"github.com/wwqdrh/datamanager/internal/driver"
	"github.com/wwqdrh/datamanager/internal/logsave/base"
	"github.com/wwqdrh/datamanager/internal/logsave/model"
)

type LogSaveLeveldb struct {
	Queue   *datautil.Queue
	LogDB   *driver.LevelDBDriver
	FnTable TransTable
	FnField TransField
}

////////////////////
// 写日志
////////////////////

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

////////////////////
// 读日志
////////////////////

// GetAllLogger 获取整体的记录行信息 还需要将关联表记录加载出来
func (s *LogSaveLeveldb) GetLogger(tableName string, primaryID string, startTime, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	res, err := new(model.LocalLog2).SearchRecordByField(s.LogDB, tableName, primaryID, startTime, endTime, page, pageSize)
	if err != nil {
		return nil, err
	}
	result := []map[string]interface{}{}
	for _, item := range res {
		log := s.transField(tableName, item["data"].([]map[string]interface{}))
		for i, c := range log {
			t, err := datautil.UnParseTime(c["time"].(string))
			if err != nil {
				continue
			}

			log[i]["relations"] = s.getRelationLogger(tableName, primaryID, t)
		}

		result = append(result, map[string]interface{}{
			"primary": s.transPrimary(tableName, strings.Split(item["key"].(string), "@")[1]),
			"log":     log,
		})
	}

	return result, nil
}

func (s *LogSaveLeveldb) transField(tableName string, records []map[string]interface{}) []map[string]interface{} {
	res := []map[string]interface{}{}

	mapRepo := new(model.FieldMapping)
	fieldMappingCache := mapRepo.ListAllFieldMapping(s.LogDB, tableName)
	for key, value := range fieldMappingCache { // 将存在的字段做更新
		fieldMappingCache[key] = s.FnField(tableName, value)
	}
	mapRepo.UpdateAllFieldByMapping(s.LogDB, tableName, fieldMappingCache)

	for _, item := range records {
		if val, ok := item["action"].(string); ok && val == "ddl" {
			res = append(res, map[string]interface{}{
				"primary":   nil,
				"data":      map[string]interface{}{"sql": item["log"]},
				"all":       nil,
				"action":    item["action"],
				"time":      item["time"],
				"relations": nil},
			)
			continue
		}

		log := item["log"].(map[string]interface{})

		primary, primaryNew := log["primary"].(map[string]interface{}), map[string]interface{}{}
		data, dataNew := log["data"].(map[string]interface{}), map[string]interface{}{}
		all, allNew := log["all"].(map[string]interface{}), map[string]interface{}{}

		for key, val := range primary {
			if newKey, ok := fieldMappingCache[key]; ok && newKey != "" {
				primaryNew[newKey] = val
			} else {
				primaryNew[key] = val
			}
		}
		for key, val := range data {
			if newKey, ok := fieldMappingCache[key]; ok && newKey != "" {
				dataNew[newKey] = val
			} else {
				dataNew[key] = val
			}
		}
		for key, val := range all {
			if newKey, ok := fieldMappingCache[key]; ok && newKey != "" {
				allNew[newKey] = val
			} else {
				allNew[key] = val
			}
		}
		res = append(res, map[string]interface{}{
			"primary":   primaryNew,
			"data":      dataNew,
			"all":       allNew,
			"action":    item["action"],
			"time":      item["time"],
			"relations": item["relations"]},
		)
	}
	return res
}

// TransPrimary tableid与name之间的映射
func (s *LogSaveLeveldb) transPrimary(tableName string, primaryStr string) string {
	if primaryStr == "type=ddl" {
		return "type=ddl"
	}

	var key, val string
	{
		t := strings.Split(primaryStr, "=")
		key, val = t[0], t[1]
	}
	newKey := []string{}
	for _, item := range strings.Split(key, ",") {
		newKey = append(newKey, s.FnField(tableName, item))
	}
	fmt.Println(key)

	return strings.Join(newKey, ",") + "=" + val
}

// GetRelationLogger 获取与当前记录有关联的记录 值相同 时间戳差值不超过1分钟
// TODO 现在的关联记录是在watcher中还没有移过来
func (s *LogSaveLeveldb) getRelationLogger(tableName string, curVal interface{}, stamp time.Time) interface{} {
	// 1、获取relations
	// var policy *base.Policy
	// if val, ok := DefaultMetaService().AllPolicy.Load(tableName); !ok {
	// 	return errors.New("未发现当前表的策略")
	// } else {
	// 	policy = val.(*stream_entity.Policy)
	// }

	// 2、根据relations中的表名，主键字段
	// start, end := stamp.Add(1*time.Second), stamp.Add(-1*time.Second) // 默认在左右一秒范围内的
	records := []map[string]interface{}{}
	// for _, item := range strings.Split(policy.Relations, ";") {
	// 	var tName, fName string
	// 	{
	// 		t := strings.Split(item, ".")
	// 		tName, fName = t[0], t[1]
	// 	}
	// 	var tpolicy *stream_entity.Policy
	// 	if val, ok := DefaultMetaService().AllPolicy.Load(tName); !ok {
	// 		return errors.New("未发现当前表的策略")
	// 	} else {
	// 		tpolicy = val.(*stream_entity.Policy)
	// 	}
	// 	// cond := s.ParsePrimaryVal(curVal.(string))
	// 	cond := map[string]string{
	// 		fName: strings.Split(curVal.(string), "=")[1],
	// 	}

	// 	primaryFields := strings.Split(tpolicy.PrimaryFields, ",")
	// 	data, err := exporter_repo.LogRepoV2.SearchRecordWithCondition(tName, primaryFields, cond, &start, &end, 0, 0)
	// 	if err != nil {
	// 		continue
	// 	}
	// 	records = append(records, data...)
	// }
	return records
}

////////////////////
// 获取所有日志
////////////////////
func (s *LogSaveLeveldb) GetAllLog(tableName string, primaryFields []string, start, end *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	res, err := new(model.LocalLog2).SearchAllRecord(s.LogDB, tableName, primaryFields, start, end, page, pageSize)
	if err != nil {
		return nil, err
	}
	result := []map[string]interface{}{}
	for _, item := range res {
		result = append(result, map[string]interface{}{
			"primary": s.transPrimary(tableName, strings.Split(item["key"].(string), "@")[1]),
			"log":     item["data"],
		})
	}

	return result, nil
}

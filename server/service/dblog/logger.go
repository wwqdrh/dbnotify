package dblog

import (
	"errors"
	"reflect"
	"strings"
	"sync"
	"time"

	"datamanager/server/common/datautil"
	"datamanager/server/common/structhandler"
	"datamanager/server/global"
	dblog_model "datamanager/server/model/dblog"
)

type loggerService struct {
	senseFields  map[string][]string //
	task         *sync.Map           // string=>bool 当前任务的存储
	logTableName string
	handler      structhandler.IStructHandler
}

// 初始化
func (s *loggerService) Init(logTableName string, handler structhandler.IStructHandler) *loggerService {
	s.senseFields = map[string][]string{}
	s.task = &sync.Map{}
	s.logTableName = logTableName
	s.handler = handler
	return s
}

// Dump 将log struct转换成map形式
func (s *loggerService) Dump(logData []*dblog_model.LogTable, primaryFields []string) ([]map[string]interface{}, error) {
	response := make([]map[string]interface{}, 0, len(logData))
	for _, item := range logData {
		data, _ := datautil.JsonToMap(string(item.Log))
		senseFields := s.senseFields[item.TableName]
		if len(senseFields) == 0 || senseFields[0] == "" {
			for _, item := range global.G_StructHandler.GetFields(item.TableName) {
				senseFields = append(senseFields, item.FieldID)
			}
		}
		log := s.wash(data, item.Action, primaryFields, senseFields...)
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

func (s *loggerService) wash(data map[string]interface{}, action string, primaryFields []string, fieldNames ...string) map[string]interface{} {
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

// SetSenseFields
func (s *loggerService) SetSenseFields(tableName string, data []string) {
	s.senseFields[tableName] = data
}

func (s *loggerService) getPolicy(s1 string) interface{} {
	if policy, ok := ServiceGroupApp.Meta.allPolicy.Load(s1); !ok {
		return nil
	} else {
		return policy
	}
}

// Run 运行
func (s *loggerService) Run(tableName string, fields []string, outdate, minLog int) error {
	if _, ok := s.task.Load(tableName); ok {
		return errors.New("已经在执行了")
	}
	policy := (s.getPolicy(tableName)).(*dblog_model.Policy)
	primaryFields := strings.Split(policy.PrimaryFields, ",")
	senseFields := strings.Split(policy.Fields, ",")

	if len(senseFields) == 0 || senseFields[0] == "" {
		return errors.New("未设置监听字段") // 未设置监听字段
	}

	s.task.Store(tableName, true)
	defer func() {
		s.task.Delete(tableName)
	}()
	// 执行读取表的逻辑 每次读200行
	data, err := dblog_model.LogTableRepo.ReadAndDeleteByID(tableName, 200)
	if err != nil {
		return err
	}

	datar, err := s.Dump(data, primaryFields)
	if err != nil {
		return err
	}

	if err = dblog_model.LogLocalLogRepo.Write(tableName, datar, fields, outdate, minLog); err != nil {
		return err
	}
	return nil
}

// GetAllLogger 获取整体的记录行信息
func (s *loggerService) GetLogger(tableName string, primaryID string, startTime, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	res, err := dblog_model.LogRepoV2.SearchRecordByField(tableName, primaryID, startTime, endTime, page, pageSize)
	if err != nil {
		return nil, err
	}
	result := []map[string]interface{}{}
	for _, item := range res {
		result = append(result, map[string]interface{}{
			"primary": s.TransPrimary(tableName, strings.Split(item["key"].(string), "@")[1]),
			"log":     s.TransField(tableName, item["data"].([]map[string]interface{})),
		})
	}

	return result, nil
}

func (s *loggerService) GetAllLogger(tableName string, primaryFields []string, start, end *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	res, err := dblog_model.LogRepoV2.SearchAllRecord(tableName, primaryFields, start, end, page, pageSize)
	if err != nil {
		return nil, err
	}
	result := []map[string]interface{}{}
	for _, item := range res {
		result = append(result, map[string]interface{}{
			"primary": s.TransPrimary(tableName, strings.Split(item["key"].(string), "@")[1]),
			"log":     item["data"],
		})
	}

	return result, nil
}

func (s *loggerService) TransField(tableName string, records []map[string]interface{}) []map[string]interface{} {
	res := []map[string]interface{}{}

	// 字段id与名字的映射 需要更新leveldb中的备份
	allfields := global.G_StructHandler.GetFields(tableName)
	fieldMapping := map[string]string{}
	for _, item := range allfields {
		fieldMapping[item.FieldID] = item.FieldName
	}
	fieldMappingCache := dblog_model.FieldMappingRepo.ListAllFieldMapping(tableName)
	for key, value := range fieldMapping { // 将存在的字段做更新
		fieldMappingCache[key] = value
	}
	dblog_model.FieldMappingRepo.UpdateAllFieldByMapping(tableName, fieldMappingCache)

	for _, item := range records {
		log := item["log"].(map[string]interface{})

		primary, primaryNew := log["primary"].(map[string]interface{}), map[string]interface{}{}
		data, dataNew := log["data"].(map[string]interface{}), map[string]interface{}{}
		all, allNew := log["all"].(map[string]interface{}), map[string]interface{}{}

		for key, val := range primary {
			if newKey, ok := fieldMapping[key]; ok && newKey != "" {
				primaryNew[newKey] = val
			} else {
				primaryNew[key] = val
			}
		}
		for key, val := range data {
			if newKey, ok := fieldMapping[key]; ok && newKey != "" {
				dataNew[newKey] = val
			} else {
				dataNew[key] = val
			}
		}
		for key, val := range all {
			if newKey, ok := fieldMapping[key]; ok && newKey != "" {
				allNew[newKey] = val
			} else {
				allNew[key] = val
			}
		}
		res = append(res, map[string]interface{}{"primary": primaryNew, "data": dataNew, "all": allNew, "action": item["action"], "time": item["time"]})
	}
	return res
}
func (s *loggerService) TransPrimary(tableName string, primaryStr string) string {
	var key, val string
	{
		t := strings.Split(primaryStr, "=")
		key, val = t[0], t[1]
	}
	newKey := []string{}
	for _, item := range strings.Split(key, ",") {
		newKey = append(newKey, s.handler.GetFieldName(tableName, item))
	}

	return strings.Join(newKey, ",") + "=" + val
}

// Load 从源数据中读取日志
func (s *loggerService) Load() func(tableName string, id uint64, num int) ([]*dblog_model.LogTable, error) {
	return func(tableName string, id uint64, num int) ([]*dblog_model.LogTable, error) {
		data, err := dblog_model.LogTableRepo.ReadBytableNameAndLimit(tableName, id, num)
		if err != nil {
			return nil, err
		}
		return data, nil
	}
}

// Store 写入到日志库
func (s *loggerService) Store() func(policy *dblog_model.Policy, data []*dblog_model.LogTable) error {
	return func(policy *dblog_model.Policy, data []*dblog_model.LogTable) error {
		primaryFields := strings.Split(policy.PrimaryFields, ",")
		datar, err := s.Dump(data, primaryFields)
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

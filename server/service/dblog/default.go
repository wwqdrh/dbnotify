package dblog

import (
	"errors"
	"reflect"
	"strings"
	"sync"
	"time"

	"datamanager/server/common/datautil"
	"datamanager/server/global"
	dblog_model "datamanager/server/model/dblog"
	"datamanager/server/types"
)

// 日志格式转换
type (
	DefaultLogger struct {
		senseFields  map[string][]string //
		task         *sync.Map           // string=>bool 当前任务的存储
		logTableName string
		getPolicy    func(string) interface{} // 根据tableName获取配置
		handler      types.IStructHandler
	}
)

func NewDefaultLogger(logTableName string, getPolicy func(string) interface{}, handler types.IStructHandler) ILogger {
	return &DefaultLogger{
		senseFields:  map[string][]string{},
		task:         &sync.Map{},
		logTableName: logTableName,
		getPolicy:    getPolicy,
		handler:      handler,
	}
}

func (l *DefaultLogger) SetSenseFields(tableName string, data []string) {
	l.senseFields[tableName] = data
}

func (l DefaultLogger) Dump(logData []*dblog_model.LogTable, primaryFields []string) ([]map[string]interface{}, error) {
	response := make([]map[string]interface{}, 0, len(logData))
	for _, item := range logData {
		data, _ := datautil.JsonToMap(string(item.Log))
		senseFields := l.senseFields[item.TableName]
		if len(senseFields) == 0 || senseFields[0] == "" {
			for _, item := range global.G_DATADB.ListTableField(item.TableName) {
				senseFields = append(senseFields, item.FieldID)
			}
		}
		log := l.wash(data, item.Action, primaryFields, senseFields...)
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

func (l DefaultLogger) Run(tableName string, fields []string, outdate, minLog int) error {
	if _, ok := l.task.Load(tableName); ok {
		return errors.New("已经在执行了")
	}
	policy := (l.getPolicy(tableName)).(*dblog_model.Policy)
	primaryFields := strings.Split(policy.PrimaryFields, ",")
	senseFields := strings.Split(policy.Fields, ",")

	if len(senseFields) == 0 || senseFields[0] == "" {
		return errors.New("未设置监听字段") // 未设置监听字段
	}

	l.task.Store(tableName, true)
	defer func() {
		l.task.Delete(tableName)
	}()
	// 执行读取表的逻辑 每次读200行
	data, err := dblog_model.LogTableRepo.ReadAndDeleteByID(tableName, 200)
	if err != nil {
		return err
	}

	datar, err := l.Dump(data, primaryFields)
	if err != nil {
		return err
	}

	if err = dblog_model.LogLocalLogRepo.Write(tableName, datar, fields, outdate, minLog); err != nil {
		return err
	}
	return nil
}

// GetLogger 获取日志格式 相同主键id的需要合并到一起
// 需要添加通过修改字段id为name的逻辑
func (l DefaultLogger) GetLogger(tableName string, primaryID string, startTime, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	res, err := dblog_model.LogRepoV2.SearchRecordByField(tableName, primaryID, startTime, endTime, page, pageSize)
	if err != nil {
		return nil, err
	}
	result := []map[string]interface{}{}
	for _, item := range res {
		result = append(result, map[string]interface{}{
			"primary": l.TransPrimary(tableName, strings.Split(item["key"].(string), "@")[1]),
			"log":     l.TransField(tableName, item["data"].([]map[string]interface{})),
		})
	}

	return result, nil
}

// GetAllLogger 获取主键的整体记录
func (l DefaultLogger) GetAllLogger(tableName string, primaryFields []string, start, end *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	res, err := dblog_model.LogRepoV2.SearchAllRecord(tableName, primaryFields, start, end, page, pageSize)
	if err != nil {
		return nil, err
	}
	result := []map[string]interface{}{}
	for _, item := range res {
		result = append(result, map[string]interface{}{
			"primary": l.TransPrimary(tableName, strings.Split(item["key"].(string), "@")[1]),
			"log":     item["data"],
		})
	}

	return result, nil
}

// TransField 转换字段
func (l DefaultLogger) TransField(tableName string, records []map[string]interface{}) []map[string]interface{} {
	res := []map[string]interface{}{}

	// 字段id与名字的映射 需要更新leveldb中的备份
	allfields := global.G_DATADB.ListTableField(tableName)
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

// TransPrimary 将1=2, 字段名转换
func (l DefaultLogger) TransPrimary(tableName string, primaryStr string) string {
	var key, val string
	{
		t := strings.Split(primaryStr, "=")
		key, val = t[0], t[1]
	}
	newKey := []string{}
	for _, item := range strings.Split(key, ",") {
		newKey = append(newKey, l.handler.GetFieldName(tableName, item))
	}

	return strings.Join(newKey, ",") + "=" + val
}

// 将run拆分
func (l DefaultLogger) Load() func(tableName string, id uint64, num int) ([]*dblog_model.LogTable, error) {
	return nil
} // 从源数据读取
func (l DefaultLogger) Store() func(policy *dblog_model.Policy, data []*dblog_model.LogTable) error {
	return nil
} // 写入到日志库中

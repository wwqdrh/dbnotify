package fulltrigger

import (
	"errors"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/wwqdrh/datamanager/runtime"

	exporter_repo "github.com/wwqdrh/datamanager/domain/exporter/repository"
	stream_entity "github.com/wwqdrh/datamanager/domain/stream/entity"
	stream_repo "github.com/wwqdrh/datamanager/domain/stream/repository"
	"github.com/wwqdrh/datamanager/internal/datautil"
)

var R = runtime.Runtime

type DbService struct {
	senseFields  map[string][]string //
	task         *sync.Map           // string=>bool 当前任务的存储
	logTableName string
	// handler      structhandler.IStructHandler
}

func NewDbService() *DbService {
	return (&DbService{
		senseFields:  make(map[string][]string, 0),
		task:         &sync.Map{},
		logTableName: R.GetConfig().TempLogTable,
		// handler:      R.GetFieldHandler(),
	})
}

// 初始化
// func (s *DbService) Init(logTableName string, handler structhandler.IStructHandler) *DbService {
// 	s.senseFields = map[string][]string{}
// 	s.task = &sync.Map{}
// 	s.logTableName = logTableName
// 	s.handler = handler
// 	return s
// }

// Dump 将log struct转换成map形式
// 需要处理ddl的log格式以及dml数据的log格式
func (s *DbService) Dump(logData []*stream_entity.LogTable, primaryFields []string) ([]map[string]interface{}, error) {
	response := make([]map[string]interface{}, 0, len(logData))
	var val map[string]interface{}
	for _, item := range logData {
		switch item.Action {
		case "ddl":
			val = s.dumpDDL(item)
		default:
			val = s.dumpDML(item, primaryFields)
		}
		if val != nil {
			response = append(response, val)
		}

	}
	return response, nil
}

// 处理ddl
func (s *DbService) dumpDDL(item *stream_entity.LogTable) map[string]interface{} {
	data, _ := datautil.JsonToMap(string(item.Log))
	return map[string]interface{}{
		"log":    data["data"].([]interface{})[0].(string), // 一般只有第一个ddl语句
		"action": item.Action,
		"time":   datautil.ParseTime(item.Time),
	}
}

// 处理dml
func (s *DbService) dumpDML(item *stream_entity.LogTable, primaryFields []string) map[string]interface{} {
	data, _ := datautil.JsonToMap(string(item.Log))
	senseFields := s.senseFields[item.TableName]
	if len(senseFields) == 0 || senseFields[0] == "" {
		for _, item := range R.GetFieldHandler().GetFields(item.TableName) {
			senseFields = append(senseFields, item.FieldID)
		}
	}
	log := s.wash(data, item.Action, primaryFields, senseFields...)
	if log == nil {
		return nil
	}
	return map[string]interface{}{
		"log":    log,
		"action": item.Action,
		"time":   datautil.ParseTime(item.Time),
	}
}

// Wash 写入日志时对数据结构的一些处理
func (s *DbService) wash(data map[string]interface{}, action string, primaryFields []string, fieldNames ...string) map[string]interface{} {
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

// SetSenseFields 设置监听的字段
func (s *DbService) SetSenseFields(tableName string, data []string) {
	s.senseFields[tableName] = data
}

// getPolicy 根据表名获取配置项
func (s *DbService) getPolicy(s1 string) interface{} {
	if policy, ok := DefaultMetaService().AllPolicy.Load(s1); !ok {
		return nil
	} else {
		return policy
	}
}

// Run 运行
func (s *DbService) Run(tableName string, fields []string, outdate, minLog int) error {
	if _, ok := s.task.Load(tableName); ok {
		return errors.New("已经在执行了")
	}
	policy := (s.getPolicy(tableName)).(*stream_entity.Policy)
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
	data, err := stream_repo.LogTableRepo.ReadAndDeleteByID(tableName, 200)
	if err != nil {
		return err
	}

	datar, err := s.Dump(data, primaryFields)
	if err != nil {
		return err
	}

	if err = exporter_repo.LogLocalLogRepo.Write(tableName, datar, fields, outdate, minLog); err != nil {
		return err
	}
	return nil
}

// GetAllLogger 获取整体的记录行信息 还需要将关联表记录加载出来
func (s *DbService) GetLogger(tableName string, primaryID string, startTime, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	res, err := exporter_repo.LogRepoV2.SearchRecordByField(tableName, primaryID, startTime, endTime, page, pageSize)
	if err != nil {
		return nil, err
	}
	result := []map[string]interface{}{}
	for _, item := range res {
		log := s.TransField(tableName, item["data"].([]map[string]interface{}))
		for i, c := range log {
			t, err := datautil.UnParseTime(c["time"].(string))
			if err != nil {
				continue
			}

			log[i]["relations"] = s.GetRelationLogger(tableName, primaryID, t)
		}

		result = append(result, map[string]interface{}{
			"primary": s.TransPrimary(tableName, strings.Split(item["key"].(string), "@")[1]),
			"log":     log,
		})
	}

	return result, nil
}

// GetAllLogger 从leveldb中获取记录 并根据策略表中的多表关系 查询相关联的记录并组合
func (s *DbService) GetAllLogger(tableName string, primaryFields []string, start, end *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	res, err := exporter_repo.LogRepoV2.SearchAllRecord(tableName, primaryFields, start, end, page, pageSize)
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

// GetRelationLogger 获取与当前记录有关联的记录 值相同 时间戳差值不超过1分钟
func (s *DbService) GetRelationLogger(tableName string, curVal interface{}, stamp time.Time) interface{} {
	// 1、获取relations
	var policy *stream_entity.Policy
	if val, ok := DefaultMetaService().AllPolicy.Load(tableName); !ok {
		return errors.New("未发现当前表的策略")
	} else {
		policy = val.(*stream_entity.Policy)
	}

	// 2、根据relations中的表名，主键字段
	start, end := stamp.Add(1*time.Second), stamp.Add(-1*time.Second) // 默认在左右一秒范围内的
	records := []map[string]interface{}{}
	for _, item := range strings.Split(policy.Relations, ";") {
		var tName, fName string
		{
			t := strings.Split(item, ".")
			tName, fName = t[0], t[1]
		}
		var tpolicy *stream_entity.Policy
		if val, ok := DefaultMetaService().AllPolicy.Load(tName); !ok {
			return errors.New("未发现当前表的策略")
		} else {
			tpolicy = val.(*stream_entity.Policy)
		}
		// cond := s.ParsePrimaryVal(curVal.(string))
		cond := map[string]string{
			fName: strings.Split(curVal.(string), "=")[1],
		}

		primaryFields := strings.Split(tpolicy.PrimaryFields, ",")
		data, err := exporter_repo.LogRepoV2.SearchRecordWithCondition(tName, primaryFields, cond, &start, &end, 0, 0)
		if err != nil {
			continue
		}
		records = append(records, data...)
	}
	return records
}

// ParsePrimaryVal id,name=1,2
func (s *DbService) ParsePrimaryVal(curVal string) map[string]string {
	var keys, vals []string
	{
		t := strings.Split(curVal, "=")
		keys, vals = strings.Split(t[0], ","), strings.Split(t[1], ",")
	}
	if len(keys) != len(vals) {
		return nil
	}
	cond := map[string]string{}
	for i := 0; i < len(keys); i++ {
		cond[keys[i]] = vals[i]
	}
	return cond
}

func (s *DbService) TransField(tableName string, records []map[string]interface{}) []map[string]interface{} {
	res := []map[string]interface{}{}

	// 字段id与名字的映射 需要更新leveldb中的备份
	allfields := R.GetFieldHandler().GetFields(tableName)
	fieldMapping := map[string]string{}
	for _, item := range allfields {
		fieldMapping[item.FieldID] = item.FieldName
	}
	fieldMappingCache := exporter_repo.FieldMappingRepo.ListAllFieldMapping(tableName)
	for key, value := range fieldMapping { // 将存在的字段做更新
		fieldMappingCache[key] = value
	}
	exporter_repo.FieldMappingRepo.UpdateAllFieldByMapping(tableName, fieldMappingCache)

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
func (s *DbService) TransPrimary(tableName string, primaryStr string) string {
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
		newKey = append(newKey, R.GetFieldHandler().GetFieldName(tableName, item))
	}

	return strings.Join(newKey, ",") + "=" + val
}

// Load 从源数据中读取日志
// 1、读取dml日志
// 2、读取ddl日志
func (s *DbService) Load() func(tableName string, id uint64, num int) ([]*stream_entity.LogTable, error) {
	return func(tableName string, id uint64, num int) ([]*stream_entity.LogTable, error) {
		data, err := stream_repo.LogTableRepo.ReadBytableNameAndLimit(tableName, id, num)
		if err != nil {
			return nil, err
		}
		return data, nil
	}
}

// Store 写入到日志库
func (s *DbService) Store() func(policy *stream_entity.Policy, data []*stream_entity.LogTable) error {
	return func(policy *stream_entity.Policy, data []*stream_entity.LogTable) error {
		primaryFields := strings.Split(policy.PrimaryFields, ",")
		datar, err := s.Dump(data, primaryFields)
		if err != nil {
			return err
		}
		if datar == nil {
			return nil // 监听字段未命中
		}

		if err = exporter_repo.LogRepoV2.Write(policy.TableName, datar, policy.Outdate, policy.MinLogNum); err != nil {
			return err
		}
		return nil
	}
}

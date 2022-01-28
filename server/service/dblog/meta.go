package dblog

import (
	"datamanager/server/common/structhandler"
	"datamanager/server/global"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	dblog_model "datamanager/server/model/dblog"
)

type MetaService struct {
	OutDate            int    // 过期时间 单位天
	MinLogNum          int    // 最少保留条数
	LogTableName       string // 日志临时表的名字
	TableStructHandler structhandler.IStructHandler
	allPolicy          *sync.Map
}

func (s *MetaService) Init() *MetaService {
	if s.LogTableName == "" {
		s.LogTableName = "action_record_log"
	}
	if s.OutDate <= 0 {
		s.OutDate = 15
	}
	s.allPolicy = new(sync.Map)
	s.OutDate = 15
	s.MinLogNum = 10
	return s
}

func (s *MetaService) InitApp(handler structhandler.IStructHandler, tables ...interface{}) (errs []error) {
	s.TableStructHandler = handler

	dblog_model.InitRepo(s.LogTableName)

	// 表策略
	dblog_model.PolicyRepo.Migrate(global.G_DATADB.DB)
	// 读取策略
	for _, item := range dblog_model.PolicyRepo.GetAllData(global.G_DATADB.DB) {
		s.allPolicy.Store(item.TableName, item)
	}
	// 添加日志表
	global.G_DATADB.DB.Table(s.LogTableName).AutoMigrate(&dblog_model.LogTable{})

	for _, table := range tables {
		s.Register(table, s.MinLogNum, s.OutDate, nil, nil)
	}

	return
}

func (s *MetaService) AddTable(table interface{}, senseFields []string, ignoreFields []string) {
	s.Register(table, s.MinLogNum, s.OutDate, senseFields, ignoreFields)
}

func (s *MetaService) GetAllPolicy() *sync.Map {
	return s.allPolicy
}

func (s *MetaService) Register(table interface{}, min_log_num, outdate int, fields []string, ignoreFields []string) error {
	s.registerCheck(table) // 检查table是否合法

	// 最少记录条数
	if min_log_num < s.MinLogNum {
		min_log_num = s.MinLogNum
	}
	if outdate < s.OutDate {
		outdate = s.OutDate
	}

	var (
		policy *dblog_model.Policy
	)
	tableName := global.G_DATADB.GetTableName(table)
	if tableName == "" {
		return errors.New("表名不能为空")
	}

	if val, ok := s.allPolicy.Load(tableName); !ok {
		primary, err := global.G_DATADB.GetPrimary(table)
		if err != nil {
			return err
		}
		primaryFields := strings.Join(primary, ",")
		fieldsStr := []string{}
		allFields := global.G_StructHandler.GetFields(tableName)
		ignoreMapping := map[string]bool{}
		for _, item := range ignoreFields {
			ignoreMapping[item] = true
		}
		if len(fields) == 0 || fields[0] == "" {
			for _, item := range allFields {
				if _, ok := ignoreMapping[item.FieldID]; !ok {
					fieldsStr = append(fieldsStr, item.FieldID)
				}
			}
		} else {
			for _, item := range fields {
				if _, ok := ignoreMapping[item]; !ok {
					fieldsStr = append(fieldsStr, item)
				}
			}
		}
		policy = &dblog_model.Policy{TableName: tableName, PrimaryFields: primaryFields, Fields: strings.Join(fieldsStr, ","), MinLogNum: min_log_num, Outdate: outdate}

		if err := dblog_model.PolicyRepo.CreateNoExist(
			global.G_DATADB.DB, policy,
			map[string]interface{}{"table_name": tableName},
		); err != nil {
			return err
		}
		s.allPolicy.Store(tableName, policy)
	} else {
		policy = val.(*dblog_model.Policy)
	}

	funcName := tableName + "_auto_log_recored()"
	triggerName := tableName + "_auto_log_trigger"

	global.G_DATADB.CreateTrigger(fmt.Sprintf(`
		create or replace FUNCTION %s RETURNS trigger
		LANGUAGE plpgsql
	    AS $$
	    declare logjson json;
	    BEGIN
	        --只有update的时候有OLD，所以必须判断操作类型为UPDATE
	        IF (TG_OP = 'UPDATE') THEN
	            --如果用户名被修改了，就插入到日志，并记录新、旧名字
	        	SELECT json_build_object(
	                'before', json_agg(old),
	                'after', json_agg(new)
	            ) into logjson;
	            INSERT INTO "%s" ("table_name", "log", "action", "time") VALUES ('%s', logjson, 'update' , CURRENT_TIMESTAMP);
	        END IF;
	        IF (TG_OP = 'DELETE') then
	        	select json_build_object('data', json_agg(old)) into logjson;
	            INSERT INTO "%s" ("table_name", "log", "action", "time") VALUES ('%s', logjson, 'delete', CURRENT_TIMESTAMP);
	        END IF;
	        IF (TG_OP = 'INSERT') then
	        	select json_build_object('data', json_agg(new)) into logjson;
	            INSERT INTO "%s" ("table_name", "log", "action", "time") VALUES ('%s', logjson, 'insert', CURRENT_TIMESTAMP);
	        END IF;
	    RETURN NEW;
	END$$;

	create trigger %s after insert or update or delete on "%s" for each row execute procedure %s;
		`, funcName,
		s.LogTableName, tableName,
		s.LogTableName, tableName,
		s.LogTableName, tableName,
		triggerName, tableName, funcName), triggerName,
	)
	ServiceGroupApp.Logger.SetSenseFields(tableName, strings.Split(policy.Fields, ","))
	return nil
}

func (s *MetaService) registerCheck(table interface{}) bool {
	if val, ok := table.(string); ok {
		// 判断表是否存在
		tables := global.G_StructHandler.GetTables()
		for _, item := range tables {
			if item.TableID == val {
				return true
			}
		}
		return false
	} else {
		global.G_DATADB.DB.AutoMigrate(table)
		return true
	}
}

func (s *MetaService) UnRegister(table string) error {
	s.registerCheck(table)

	if err := dblog_model.PolicyRepo.DeleteByTableName(global.G_DATADB.DB, table); err != nil {
		return errors.New("删除记录失败")
	}
	if err := global.G_DATADB.DeleteTrigger(table + "_auto_log_trigger"); err != nil {
		return errors.New("删除触发器失败")
	}
	s.allPolicy.Delete(table)
	return nil
}

func (s *MetaService) ListTable() []string {
	res := []string{}
	s.allPolicy.Range(func(key, value interface{}) bool {
		item := value.(*dblog_model.Policy)
		res = append(res, item.TableName)
		return true
	})
	return res
}

func (s *MetaService) ListTable2() []*structhandler.Table {
	result := global.G_StructHandler.GetTables()

	tableMapping := map[string]*structhandler.Table{}
	for _, item := range result {
		tableMapping[item.TableName] = item
	}
	s.allPolicy.Range(func(key, value interface{}) bool {
		item := value.(*dblog_model.Policy)
		if val, ok := tableMapping[item.TableName]; ok {
			val.IsListen = true
		}
		return true
	})

	return result
}

func (s *MetaService) ListTableField(tableName string) []*structhandler.Fields {
	return s.TableStructHandler.GetFields(tableName)
}

func (s *MetaService) ListTableAllLog(tableName string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	if val, ok := s.allPolicy.Load(tableName); !ok {
		return nil, errors.New(tableName + "未进行监听")
	} else {
		policy := val.(*dblog_model.Policy)
		primaryFields := strings.Split(policy.PrimaryFields, ",")
		return ServiceGroupApp.Logger.GetAllLogger(tableName, primaryFields, startTime, endTime, page, pageSize)
	}
}

func (s *MetaService) ListTableLog(tableName string, recordID string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	if _, ok := s.allPolicy.Load(tableName); !ok {
		return nil, errors.New(tableName + "未进行监听")
	} else {
		// policy := val.(*model.Policy)
		return ServiceGroupApp.Logger.GetLogger(tableName, recordID, startTime, endTime, page, pageSize)
	}
}

func (s *MetaService) ModifyPolicy(tableName string, args map[string]interface{}) error {
	var policy *dblog_model.Policy
	if val, ok := s.allPolicy.Load(tableName); !ok {
		return errors.New("表未注册")
	} else {
		policy = val.(*dblog_model.Policy)
	}

	if val, ok := args["out_date"].(int); ok && val != 0 {
		policy.Outdate = val
	}
	if val, ok := args["fields"].([]string); ok && val != nil {
		policy.Fields = strings.Join(val, ",")
	}
	if val, ok := args["min_log_num"].(int); ok && val != 0 {
		policy.MinLogNum = val
	}

	if err := global.G_DATADB.DB.Save(policy).Error; err != nil {
		return err
	}
	return nil
}

func (s *MetaService) ModifyOutdate(tableName string, outdate int) error {
	var policy *dblog_model.Policy
	if val, ok := s.allPolicy.Load(tableName); !ok {
		return errors.New("表未注册")
	} else {
		policy = val.(*dblog_model.Policy)
	}
	if policy.Outdate == outdate {
		return nil
	}

	policy.Outdate = outdate
	if err := global.G_DATADB.DB.Save(policy).Error; err != nil {
		return err
	}
	return nil
}

func (s *MetaService) ModifyField(tableName string, fields []string) error {
	var policy *dblog_model.Policy
	if val, ok := s.allPolicy.Load(tableName); !ok {
		return errors.New("表未注册")
	} else {
		policy = val.(*dblog_model.Policy)
	}
	policy.Fields = strings.Join(fields, ",")
	ServiceGroupApp.Logger.SetSenseFields(tableName, fields)
	return global.G_DATADB.DB.Save(policy).Error
}

func (s *MetaService) ModifyMinLogNum(tableName string, minLogNum int) error {
	var policy *dblog_model.Policy
	if val, ok := s.allPolicy.Load(tableName); !ok {
		return errors.New("表未注册")
	} else {
		policy = val.(*dblog_model.Policy)
	}
	policy.MinLogNum = minLogNum
	return global.G_DATADB.DB.Save(policy).Error
}

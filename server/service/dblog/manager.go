package dblog

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"datamanager/server/global"
	dblog_model "datamanager/server/model/dblog"
	"datamanager/server/types"
)

var (
	allPolicy *sync.Map = &sync.Map{} //  map[string]*model.Policy
)

type (
	ManagerCore struct {
		*ManagerConf
	}

	ManagerConf struct {
		OutDate            int    // 过期时间 单位天
		MinLogNum          int    // 最少保留条数
		LogTableName       string // 日志临时表的名字
		LoggerPolicy       ILogger
		TableStructHandler types.IStructHandler
	}
)

func GetAllPolicy() *sync.Map {
	return allPolicy
}

func NewManagerCore(conf *ManagerConf) *ManagerCore {
	return &ManagerCore{
		ManagerConf: conf,
	}
}

func (c *ManagerConf) Init() *ManagerConf {
	if c.LogTableName == "" {
		c.LogTableName = "action_record_log"
	}
	if c.OutDate <= 0 {
		c.OutDate = 15
	}
	if c.LoggerPolicy == nil {
		c.LoggerPolicy = NewConsumerLogger(c.LogTableName, c.TableStructHandler, func(s1 string) interface{} {
			if policy, ok := allPolicy.Load(s1); !ok {
				return nil
			} else {
				return policy
			}
		})
	}

	dblog_model.InitRepo(c.LogTableName)

	return c
}

// 自动处理需要的表结构
func (m *ManagerCore) Init() *ManagerCore {
	dblog_model.PolicyRepo.Migrate(global.G_DATADB.DB)
	// 读取策略
	for _, item := range dblog_model.PolicyRepo.GetAllData(global.G_DATADB.DB) {
		allPolicy.Store(item.TableName, item)
	}
	// 添加日志表
	global.G_DATADB.DB.Table(m.LogTableName).AutoMigrate(&dblog_model.LogTable{})

	return m
}

// table
// 1、struct
// 2、string
func (m *ManagerCore) Register(table interface{}, min_log_num, outdate int, fields []string, ignoreFields []string) error {
	m.registerCheck(table) // 检查table是否合法

	// 最少记录条数
	if min_log_num < m.MinLogNum {
		min_log_num = m.MinLogNum
	}
	if outdate < m.OutDate {
		outdate = m.OutDate
	}

	var (
		policy *dblog_model.Policy
	)
	tableName := global.G_DATADB.GetTableName(table)
	if tableName == "" {
		return errors.New("表名不能为空")
	}

	if val, ok := allPolicy.Load(tableName); !ok {
		primary, err := global.G_DATADB.GetPrimary(table)
		if err != nil {
			return err
		}
		primaryFields := strings.Join(primary, ",")
		fieldsStr := []string{}
		allFields := global.G_DATADB.ListTableField(tableName)
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
		allPolicy.Store(tableName, policy)
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
		m.LogTableName, tableName,
		m.LogTableName, tableName,
		m.LogTableName, tableName,
		triggerName, tableName, funcName), triggerName,
	)
	m.LoggerPolicy.SetSenseFields(tableName, strings.Split(policy.Fields, ","))

	return nil
}

// 取消监听 删除策略表中的记录 删除触发器 删除本地缓存
func (m *ManagerCore) UnRegister(table string) error {
	m.registerCheck(table)

	if err := dblog_model.PolicyRepo.DeleteByTableName(global.G_DATADB.DB, table); err != nil {
		return errors.New("删除记录失败")
	}
	if err := global.G_DATADB.DeleteTrigger(table + "_auto_log_trigger"); err != nil {
		return errors.New("删除触发器失败")
	}
	allPolicy.Delete(table)
	return nil
}

func (m *ManagerCore) registerCheck(table interface{}) bool {
	if val, ok := table.(string); ok {
		// 判断表是否存在
		tables := global.G_DATADB.ListTable()
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

// 获取监听的所有数据表的名字
func (m *ManagerCore) ListTable() []string {
	res := []string{}
	allPolicy.Range(func(key, value interface{}) bool {
		item := value.(*dblog_model.Policy)
		res = append(res, item.TableName)
		return true
	})
	return res
}

// ListTable2 根据设置的回调获取表结构
// 数据库中所有表都返回 添加标识标明是否被监听
func (m *ManagerCore) ListTable2() []*types.Table {
	result := global.G_DATADB.ListTable()

	tableMapping := map[string]*types.Table{}
	for _, item := range result {
		tableMapping[item.TableName] = item
	}
	allPolicy.Range(func(key, value interface{}) bool {
		item := value.(*dblog_model.Policy)
		if val, ok := tableMapping[item.TableName]; ok {
			val.IsListen = true
		}
		return true
	})

	return result
}

func (m *ManagerCore) ListTableField(tableName string) []*types.Fields {
	return m.TableStructHandler.GetFields(tableName)
}

func (m *ManagerCore) ListTableAllLog(tableName string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	if val, ok := allPolicy.Load(tableName); !ok {
		return nil, errors.New(tableName + "未进行监听")
	} else {
		policy := val.(*dblog_model.Policy)
		primaryFields := strings.Split(policy.PrimaryFields, ",")
		return m.LoggerPolicy.GetAllLogger(tableName, primaryFields, startTime, endTime, page, pageSize)
	}
}

// 从leveldb中获取
func (m *ManagerCore) ListTableLog(tableName string, recordID string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	if _, ok := allPolicy.Load(tableName); !ok {
		return nil, errors.New(tableName + "未进行监听")
	} else {
		// policy := val.(*model.Policy)
		return m.LoggerPolicy.GetLogger(tableName, recordID, startTime, endTime, page, pageSize)
	}
}

func (m *ManagerCore) ModifyPolicy(tableName string, args map[string]interface{}) error {
	var policy *dblog_model.Policy
	if val, ok := allPolicy.Load(tableName); !ok {
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

func (m *ManagerCore) ModifyOutdate(tableName string, outdate int) error {
	var policy *dblog_model.Policy
	if val, ok := allPolicy.Load(tableName); !ok {
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

func (m *ManagerCore) ModifyField(tableName string, fields []string) error {
	var policy *dblog_model.Policy
	if val, ok := allPolicy.Load(tableName); !ok {
		return errors.New("表未注册")
	} else {
		policy = val.(*dblog_model.Policy)
	}
	policy.Fields = strings.Join(fields, ",")
	m.LoggerPolicy.SetSenseFields(tableName, fields)
	return global.G_DATADB.DB.Save(policy).Error
}

// ModifyMinLogNum: 修改最少保留日志数
// 修改配置
func (m *ManagerCore) ModifyMinLogNum(tableName string, minLogNum int) error {
	var policy *dblog_model.Policy
	if val, ok := allPolicy.Load(tableName); !ok {
		return errors.New("表未注册")
	} else {
		policy = val.(*dblog_model.Policy)
	}
	policy.MinLogNum = minLogNum
	return global.G_DATADB.DB.Save(policy).Error
}

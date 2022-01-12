package manager

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"datamanager/manager/logger"
	"datamanager/manager/model"
	"datamanager/pkg/plugger"
	"datamanager/types"
)

var (
	allPolicy *sync.Map = &sync.Map{} //  map[string]*model.Policy
)

type (
	ManagerCore struct {
		*ManagerConf
	}

	ManagerConf struct {
		TargetDB           *plugger.PostgresDriver
		LogDB              *plugger.LevelDBDriver
		OutDate            int    // 过期时间 单位天
		LogTableName       string // 日志临时表的名字
		LoggerPolicy       logger.ILogger
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
		c.LoggerPolicy = logger.NewConsumerLogger(c.TargetDB, c.LogDB, c.LogTableName, c.TableStructHandler, func(s1 string) interface{} {
			if policy, ok := allPolicy.Load(s1); !ok {
				return nil
			} else {
				return policy
			}
		})
	}

	model.InitRepo(c.LogDB, c.TargetDB, c.LogTableName)

	return c
}

// 自动处理需要的表结构
func (m *ManagerCore) Init() *ManagerCore {
	model.PolicyRepo.Migrate(m.TargetDB.DB)
	// 读取策略
	for _, item := range model.PolicyRepo.GetAllData(m.TargetDB.DB) {
		allPolicy.Store(item.TableName, item)
	}
	// 添加日志表
	m.TargetDB.DB.Table(m.LogTableName).AutoMigrate(&model.LogTable{})

	return m
}

func (m *ManagerCore) Register(table interface{}) error {
	var (
		policy *model.Policy
	)
	tableName := m.TargetDB.GetTableName(table)
	if tableName == "" {
		return errors.New("表名不能为空")
	}

	if val, ok := allPolicy.Load(tableName); !ok {
		primary, err := m.TargetDB.GetPrimary(table)
		if err != nil {
			return err
		}
		primaryFields := strings.Join(primary, ",")
		senseFields := strings.Join(m.TargetDB.ListTableField(tableName), ",")
		policy = &model.Policy{TableName: tableName, PrimaryFields: primaryFields, Fields: senseFields, Outdate: m.OutDate}

		if err := model.PolicyRepo.CreateNoExist(
			m.TargetDB.DB, policy,
			map[string]interface{}{"table_name": tableName},
		); err != nil {
			return err
		}
		allPolicy.Store(tableName, policy)
	} else {
		policy = val.(*model.Policy)
	}

	m.TargetDB.DB.AutoMigrate(table)
	funcName := tableName + "_auto_log_recored()"
	triggerName := tableName + "_auto_log_trigger"

	m.TargetDB.CreateTrigger(fmt.Sprintf(`
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

create trigger %s after insert or update or delete on %s for each row execute procedure %s;
	`, funcName,
		m.LogTableName, tableName,
		m.LogTableName, tableName,
		m.LogTableName, tableName,
		triggerName, tableName, funcName), triggerName,
	)
	m.LoggerPolicy.SetSenseFields(tableName, strings.Split(policy.Fields, ","))

	return nil
}

// 获取监听的所有数据表的名字
func (m *ManagerCore) ListTable() []string {
	res := []string{}
	allPolicy.Range(func(key, value interface{}) bool {
		item := value.(*model.Policy)
		res = append(res, item.TableName)
		return true
	})
	return res
}

// ListTable2 根据设置的回调获取表结构
func (m *ManagerCore) ListTable2() []*types.Table {
	return m.TableStructHandler.GetTables()
}

func (m *ManagerCore) ListTableField(tableName string) []*types.Fields {
	return m.TableStructHandler.GetFields(tableName)
}

// 从leveldb中获取
func (m *ManagerCore) ListTableLog(tableName string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	if val, ok := allPolicy.Load(tableName); !ok {
		return nil, errors.New(tableName + "未进行监听")
	} else {
		policy := val.(*model.Policy)
		primaryFields := strings.Split(policy.PrimaryFields, ",")
		return m.LoggerPolicy.GetLogger(tableName, primaryFields, startTime, endTime, page, pageSize)
	}
}

func (m *ManagerCore) ModifyPolicy(tableName string, args map[string]interface{}) error {
	var policy *model.Policy
	if val, ok := allPolicy.Load(tableName); !ok {
		return errors.New("表未注册")
	} else {
		policy = val.(*model.Policy)
	}

	if val, ok := args["out_date"]; ok {
		policy.Outdate = val.(int)
	}
	if val, ok := args["fields"]; ok {
		policy.Fields = val.(string)
	}
	if val, ok := args["min_log_num"]; ok {
		policy.MinLogNum = val.(int)
	}

	if err := m.TargetDB.DB.Save(policy).Error; err != nil {
		return err
	}
	return nil
}

func (m *ManagerCore) ModifyOutdate(tableName string, outdate int) error {
	var policy *model.Policy
	if val, ok := allPolicy.Load(tableName); !ok {
		return errors.New("表未注册")
	} else {
		policy = val.(*model.Policy)
	}
	if policy.Outdate == outdate {
		return nil
	}

	policy.Outdate = outdate
	if err := m.TargetDB.DB.Save(policy).Error; err != nil {
		return err
	}
	return nil
}

func (m *ManagerCore) ModifyField(tableName string, fields []string) error {
	var policy *model.Policy
	if val, ok := allPolicy.Load(tableName); !ok {
		return errors.New("表未注册")
	} else {
		policy = val.(*model.Policy)
	}
	policy.Fields = strings.Join(fields, ",")
	m.LoggerPolicy.SetSenseFields(tableName, fields)
	return m.TargetDB.DB.Save(policy).Error
}

// ModifyMinLogNum: 修改最少保留日志数
// 修改配置
func (m *ManagerCore) ModifyMinLogNum(tableName string, minLogNum int) error {
	var policy *model.Policy
	if val, ok := allPolicy.Load(tableName); !ok {
		return errors.New("表未注册")
	} else {
		policy = val.(*model.Policy)
	}
	policy.MinLogNum = minLogNum
	return m.TargetDB.DB.Save(policy).Error
}

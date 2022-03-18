package fulltrigger

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/wwqdrh/datamanager/core"
	stream_entity "github.com/wwqdrh/datamanager/domain/stream/entity"
	stream_repo "github.com/wwqdrh/datamanager/domain/stream/repository"
	"github.com/wwqdrh/datamanager/domain/stream/vo"
	"github.com/wwqdrh/datamanager/internal/structhandler"
)

var MetaService *metaService = &metaService{}

type metaService struct {
	OutDate      int    // 过期时间 单位天
	MinLogNum    int    // 最少保留条数
	LogTableName string // 日志临时表的名字
	AllPolicy    *sync.Map
}

// Init 初始化metaService
func (s *metaService) Init() *metaService {
	if s.LogTableName == "" {
		s.LogTableName = core.G_CONFIG.DataLog.LogTableName
	}
	if s.OutDate <= 0 {
		s.OutDate = 15
	}
	s.AllPolicy = new(sync.Map)
	s.OutDate = core.G_CONFIG.DataLog.OutDate
	s.MinLogNum = core.G_CONFIG.DataLog.MinLogNum
	return s
}

// 初始化db的触发器等
func (s *metaService) InitialDB() error {
	return core.G_DATADB.DB.Exec(`
	CREATE EXTENSION IF NOT EXISTS hstore;
	CREATE SCHEMA IF NOT EXISTS action_log;
	CREATE TABLE IF NOT EXISTS action_log.ddl (
	  id        serial NOT NULL,
	  crt_time  timestamp WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	  ctx       public.hstore,
	  sql       text,
	  tg_type   varchar(200),
	  tg_event  varchar(200),
	  /* Keys */
	  CONSTRAINT aud_alter_pkey
		PRIMARY KEY (id)
	) WITH (OIDS = FALSE);
	-- 事件触发器函数
	CREATE OR REPLACE FUNCTION ddl_end_log_function()     
	  RETURNS event_trigger                    
	 LANGUAGE plpgsql  
	  AS $$  
	  declare 
			 rec hstore;  
	BEGIN  
	  --RAISE NOTICE 'this is etgr1, event:%, command:%', tg_event, tg_tag; 
	  select hstore(pg_stat_activity.*) into rec from pg_stat_activity where pid=pg_backend_pid();
	  insert into %s ("table_name", "log", "action", "time") 
	  values (SELECT
		now(),
		classid,
		objid,
		objsubid,
		command_tag,
		object_type,
		schema_name,
		object_identity,
		in_extension
		FROM pg_event_trigger_ddl_commands() left join select(rec,rec->'query',tg_tag,tg_event)); 
	 END;  
	$$;  
	drop event trigger if exists ddl_end_log_trigger;
	create EVENT TRIGGER ddl_end_log_trigger ON ddl_command_end when TAG IN ('CREATE TABLE', 'DROP TABLE', 'ALTER TABLE') EXECUTE PROCEDURE ddl_end_log_function();  
	`, s.LogTableName).Error
}

// InitApp 读取表的策略
func (s *metaService) InitApp(tables ...*vo.TablePolicy) (errs []error) {
	// 表策略
	stream_repo.PolicyRepo.Migrate(core.G_DATADB.DB)
	// 读取策略
	for _, item := range stream_repo.PolicyRepo.GetAllData(core.G_DATADB.DB) {
		s.AllPolicy.Store(item.TableName, item)
	}
	// 添加日志表
	core.G_DATADB.DB.Table(s.LogTableName).AutoMigrate(&stream_entity.LogTable{})

	for _, table := range tables {
		// s.Register(table, s.MinLogNum, s.OutDate, nil, nil)
		if err := s.RegisterWithPolicy(table); err != nil {
			fmt.Println(err)
		}
	}

	return
}

// AddTable 添加动态变
func (s *metaService) AddTable(table interface{}, senseFields []string, ignoreFields []string) {
	s.RegisterWithPolicy(&vo.TablePolicy{
		Table:        table,
		SenseFields:  senseFields,
		IgnoreFields: ignoreFields,
	})
}

func (s *metaService) GetAllPolicy() *sync.Map {
	return s.AllPolicy
}

func (s *metaService) Register(table interface{}, min_log_num, outdate int, fields []string, ignoreFields []string) error {
	return s.RegisterWithPolicy(&vo.TablePolicy{
		Table:        table,
		SenseFields:  fields,
		IgnoreFields: ignoreFields,
	})
}

func (s *metaService) RegisterWithPolicy(pol *vo.TablePolicy) error {
	table := pol.Table
	if !s.registerCheck(table) { // 检查table是否合法
		return fmt.Errorf("%v不合法", table)
	}

	tableName := core.G_DATADB.GetTableName(table)
	if tableName == "" {
		return errors.New("表名不能为空")
	}

	if _, ok := s.AllPolicy.Load(tableName); !ok {
		if p, err := s.buildPolicy(tableName, s.MinLogNum, s.OutDate, pol); err != nil {
			return err
		} else {
			DblogService.SetSenseFields(tableName, strings.Split(p.Fields, ","))
		}

		if err := s.buildTrigger(tableName); err != nil {
			return err
		}
	}
	return nil
}

// buildPolicy 为新表注册策略
func (s *metaService) buildPolicy(tableName string, min_log_num, outdate int, pol *vo.TablePolicy) (*stream_entity.Policy, error) {
	// 最少记录条数
	if min_log_num < s.MinLogNum {
		min_log_num = s.MinLogNum
	}
	if outdate < s.OutDate {
		outdate = s.OutDate
	}
	primary, err := core.G_DATADB.GetPrimary(tableName)
	if err != nil {
		return nil, err
	}
	primaryFields := strings.Join(primary, ",")
	fieldsStr := []string{}
	allFields := core.G_StructHandler.GetFields(tableName)
	ignoreMapping := map[string]bool{}
	for _, item := range pol.IgnoreFields {
		ignoreMapping[item] = true
	}
	if len(pol.SenseFields) == 0 || pol.SenseFields[0] == "" {
		for _, item := range allFields {
			if _, ok := ignoreMapping[item.FieldID]; !ok {
				fieldsStr = append(fieldsStr, item.FieldID)
			}
		}
	} else {
		for _, item := range pol.SenseFields {
			if _, ok := ignoreMapping[item]; !ok {
				fieldsStr = append(fieldsStr, item)
			}
		}
	}
	policy := &stream_entity.Policy{
		TableName:     tableName,
		PrimaryFields: primaryFields,
		Fields:        strings.Join(fieldsStr, ","),
		MinLogNum:     min_log_num,
		Outdate:       outdate,
		RelaField:     pol.RelaField,
		Relations:     pol.Relations,
	}

	if err := stream_repo.PolicyRepo.CreateNoExist(
		core.G_DATADB.DB, policy,
		map[string]interface{}{"table_name": tableName},
	); err != nil {
		return nil, err
	}
	s.AllPolicy.Store(tableName, policy)
	return policy, nil
}

func (s *metaService) buildTrigger(tableName string) error {
	funcName := tableName + "_auto_log_recored()"
	triggerName := tableName + "_auto_log_trigger"

	if err := core.G_DATADB.CreateTrigger(fmt.Sprintf(`
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
	); err != nil {
		return err
	}

	// 记录表结构变更
	if err := core.G_DATADB.CreateEventTrigger(fmt.Sprintf(`
	CREATE EXTENSION IF NOT EXISTS hstore;
	CREATE OR REPLACE FUNCTION ddl_end_log_function()     
	  RETURNS event_trigger                    
	 LANGUAGE plpgsql  
	  AS $$  
	  declare 
			 rec hstore;  
			 logjson json;
			 t varchar(255);
	BEGIN  
	  select hstore(pg_stat_activity.*) into rec from pg_stat_activity where pid=pg_backend_pid();
	  t := SPLIT_PART((select object_identity FROM pg_event_trigger_ddl_commands() where object_type = 'table'), '.', 2);
	  select json_build_object('data', json_agg(rec->'query')) into logjson;

	  insert into %s ("table_name", "log", "action", "time") 
	  values (t, logjson, 'ddl', CURRENT_TIMESTAMP); 
	 END;  
	$$; 
	create EVENT TRIGGER ddl_end_log_trigger ON ddl_command_end when TAG IN ('CREATE TABLE', 'ALTER TABLE') EXECUTE PROCEDURE ddl_end_log_function();
	`, s.LogTableName), "ddl_end_log_trigger"); err != nil {
		return err
	}

	if err := core.G_DATADB.CreateEventTrigger(fmt.Sprintf(`
	CREATE EXTENSION IF NOT EXISTS hstore;
	CREATE OR REPLACE FUNCTION ddl_drop_log_function()     
	RETURNS event_trigger                    
	LANGUAGE plpgsql  
	AS $$  
	declare 
		rec hstore;  
		logjson json;
		t varchar(255);
	BEGIN  
	select hstore(pg_stat_activity.*) into rec from pg_stat_activity where pid=pg_backend_pid();
	t := SPLIT_PART((select object_identity FROM pg_event_trigger_dropped_objects() where object_type in ('table', 'table column') limit 1), '.', 2);
	select json_build_object('data', json_agg(rec->'query')) into logjson;

	insert into %s ("table_name", "log", "action", "time") 
	values (t, logjson, 'ddl', CURRENT_TIMESTAMP); 
	END;  
	$$; 
	CREATE EVENT TRIGGER ddl_sql_drop_trigger on sql_drop EXECUTE PROCEDURE ddl_drop_log_function();
	`, s.LogTableName), "ddl_sql_drop_trigger"); err != nil {
		return err
	}

	return nil
}

func (s *metaService) registerCheck(table interface{}) bool {
	if val, ok := table.(string); ok {
		// 判断表是否存在
		tables := core.G_StructHandler.GetTables()
		for _, item := range tables {
			if item.TableID == val {
				return true
			}
		}
		return false
	} else {
		core.G_DATADB.DB.AutoMigrate(table)
		return true
	}
}

func (s *metaService) UnRegister(table string) error {
	s.registerCheck(table)

	if err := stream_repo.PolicyRepo.DeleteByTableName(core.G_DATADB.DB, table); err != nil {
		return errors.New("删除记录失败")
	}
	if err := core.G_DATADB.DeleteTrigger(table + "_auto_log_trigger"); err != nil {
		return errors.New("删除触发器失败")
	}
	s.AllPolicy.Delete(table)
	return nil
}

func (s *metaService) ListTable() []string {
	res := []string{}
	s.AllPolicy.Range(func(key, value interface{}) bool {
		item := value.(*stream_entity.Policy)
		res = append(res, item.TableName)
		return true
	})
	return res
}

func (s *metaService) ListTableField(tableName string) []*structhandler.Fields {
	if core.G_StructHandler == nil {
		return nil
	}
	return core.G_StructHandler.GetFields(tableName)
}

func (s *metaService) ListTableAllLog(tableName string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	if val, ok := s.AllPolicy.Load(tableName); !ok {
		return nil, errors.New(tableName + "未进行监听")
	} else {
		policy := val.(*stream_entity.Policy)
		primaryFields := strings.Split(policy.PrimaryFields, ",")
		return DblogService.GetAllLogger(tableName, primaryFields, startTime, endTime, page, pageSize)
	}
}

func (s *metaService) ListTableLog(tableName string, recordID string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	if _, ok := s.AllPolicy.Load(tableName); !ok {
		return nil, errors.New(tableName + "未进行监听")
	} else {
		// policy := val.(*model.Policy)
		return DblogService.GetLogger(tableName, recordID, startTime, endTime, page, pageSize)
	}
}

func (s *metaService) ModifyPolicy(tableName string, args map[string]interface{}) error {
	var policy *stream_entity.Policy
	if val, ok := s.AllPolicy.Load(tableName); !ok {
		return errors.New("表未注册")
	} else {
		policy = val.(*stream_entity.Policy)
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

	if err := core.G_DATADB.DB.Save(policy).Error; err != nil {
		return err
	}
	return nil
}

func (s *metaService) ModifyOutdate(tableName string, outdate int) error {
	var policy *stream_entity.Policy
	if val, ok := s.AllPolicy.Load(tableName); !ok {
		return errors.New("表未注册")
	} else {
		policy = val.(*stream_entity.Policy)
	}
	if policy.Outdate == outdate {
		return nil
	}

	policy.Outdate = outdate
	if err := core.G_DATADB.DB.Save(policy).Error; err != nil {
		return err
	}
	return nil
}

func (s *metaService) ModifyField(tableName string, fields []string) error {
	var policy *stream_entity.Policy
	if val, ok := s.AllPolicy.Load(tableName); !ok {
		return errors.New("表未注册")
	} else {
		policy = val.(*stream_entity.Policy)
	}
	policy.Fields = strings.Join(fields, ",")
	DblogService.SetSenseFields(tableName, fields)
	return core.G_DATADB.DB.Save(policy).Error
}

func (s *metaService) ModifyMinLogNum(tableName string, minLogNum int) error {
	var policy *stream_entity.Policy
	if val, ok := s.AllPolicy.Load(tableName); !ok {
		return errors.New("表未注册")
	} else {
		policy = val.(*stream_entity.Policy)
	}
	policy.MinLogNum = minLogNum
	return core.G_DATADB.DB.Save(policy).Error
}

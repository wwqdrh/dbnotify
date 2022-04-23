package policytable

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wwqdrh/datamanager/internal/datautil"
	"github.com/wwqdrh/datamanager/internal/driver"
	"github.com/wwqdrh/datamanager/internal/pgwatcher/base"
)

type PgwatcherTable struct {
	DB           *driver.PostgresDriver
	AllPolicy    *sync.Map
	Outdate      int    // 保存的记录时间
	MinLogNum    int    // 最少保留的日志数
	TempLogTable string // 临时日志名字
	PerReadNum   int    // 一次读取多少条
	Handler      base.IStructHandler

	Readch *datautil.Queue
}

// 使用不同的策略进行注册
// 创建触发器
// 创建记录表
// 创建策略表
func (p *PgwatcherTable) Initail() error {
	if err := p.DB.DB.Raw(`
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
	`, p.TempLogTable).Error; err != nil {
		return err
	}
	new(Policy).Migrate(p.DB.DB)
	new(LogTable).Migrate(p.DB.DB)

	// 读取现有的策略
	if p.AllPolicy == nil {
		p.AllPolicy = &sync.Map{}
	}
	for _, item := range new(Policy).GetAllData(p.DB.DB) {
		p.AllPolicy.Store(item.TableName, item)
	}
	return nil
}

// 注册表，为表创建触发器
// 记录表的日志策略
func (p *PgwatcherTable) Register(policy *base.TablePolicy) error {
	table := policy.Table
	// if !s.registerCheck(table) { // 检查table是否合法
	// 	return fmt.Errorf("%v不合法", table)
	// }

	tableName := p.DB.GetTableName(table)
	if tableName == "" {
		return errors.New("表名不能为空")
	}

	if _, ok := p.AllPolicy.Load(tableName); !ok {
		if policy, err := NewPolicy(p.DB, tableName, p.MinLogNum, p.Outdate, policy, p.Handler); err != nil {
			return err
		} else {
			p.AllPolicy.Store(tableName, policy)
			// NewDbService().SetSenseFields(tableName, strings.Split(p.Fields, ","))
		}
		if err := p.buildTrigger(tableName); err != nil {
			return err
		}

	}
	return nil
}

// 为表创建触发器
func (p *PgwatcherTable) buildTrigger(tableName string) error {
	funcName := tableName + "_auto_log_recored()"
	triggerName := tableName + "_auto_log_trigger"

	if err := p.DB.CreateTrigger(fmt.Sprintf(`
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
		p.TempLogTable, tableName,
		p.TempLogTable, tableName,
		p.TempLogTable, tableName,
		triggerName, tableName, funcName), triggerName,
	); err != nil {
		return err
	}

	// 记录表结构变更
	if err := p.DB.CreateEventTrigger(fmt.Sprintf(`
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
	`, p.TempLogTable), "ddl_end_log_trigger"); err != nil {
		return err
	}

	if err := p.DB.CreateEventTrigger(fmt.Sprintf(`
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
	`, p.TempLogTable), "ddl_sql_drop_trigger"); err != nil {
		return err
	}

	return nil
}

// 为表删除触发器
func (p *PgwatcherTable) deleteTrigger(tableName string) error {
	return nil
}

// 所有的表 包括动态创建的表
func (p *PgwatcherTable) ListenAll() chan interface{} {
	go func() {
		var id uint64
		for {
			p.AllPolicy.Range(func(key, value interface{}) bool {
				tableName := key.(string)
				policy := value.(*Policy)
				res, err := p.readLog(tableName, id, p.MinLogNum)
				if err != nil {
					fmt.Println("获取数据失败", err.Error())
					return true
				}
				if len(res) == 0 {
					return true
				}

				if !<-p.Readch.Push(map[string]interface{}{
					"data":   res,
					"policy": policy,
				}) {
					return true
				}
				if err := new(LogTable).DeleteByID(p.DB.DB, p.TempLogTable, res); err != nil {
					fmt.Println("删除数据失败:", err.Error())
					return true
				}

				if res[len(res)-1].ID > id {
					id = res[len(res)-1].ID
				}
				return true
			})
			time.Sleep(time.Minute * 10)
		}

	}()
	return nil
}

func (p *PgwatcherTable) readLog(tableName string, id uint64, num int) ([]*LogTable, error) {
	data, err := new(LogTable).ReadBytableNameAndLimit(p.DB.DB, p.TempLogTable, tableName, id, num)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// 以table粒度进行监听
func (p *PgwatcherTable) ListenTable(tableName string) chan interface{} {
	panic("not implemented") // TODO: Implement
}

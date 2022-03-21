package triggerstream

import (
	"fmt"
	"strings"

	"github.com/wwqdrh/datamanager/core"
	"github.com/wwqdrh/datamanager/domain/stream/entity"
	"github.com/wwqdrh/datamanager/domain/stream/repository"
	"github.com/wwqdrh/datamanager/domain/stream/vo"
)

// buildPolicy 为新表注册策略
func buildPolicy(tableName string, min_log_num, outdate int, pol vo.TablePolicy) (*entity.Policy, error) {
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
	policy := &entity.Policy{
		TableName:     tableName,
		PrimaryFields: primaryFields,
		Fields:        strings.Join(fieldsStr, ","),
		MinLogNum:     min_log_num,
		Outdate:       outdate,
		RelaField:     pol.RelaField,
		Relations:     pol.Relations,
	}

	if err := repository.PolicyRepo.CreateNoExist(
		core.G_DATADB.DB, policy,
		map[string]interface{}{"table_name": tableName},
	); err != nil {
		return nil, err
	}
	// s.AllPolicy.Store(tableName, policy)
	return policy, nil
}

// 构造trigger字符串
func buildTrigger(tableName, logTableName string) error {
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
		logTableName, tableName,
		logTableName, tableName,
		logTableName, tableName,
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
	`, logTableName), "ddl_end_log_trigger"); err != nil {
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
	`, logTableName), "ddl_sql_drop_trigger"); err != nil {
		return err
	}

	return nil
}

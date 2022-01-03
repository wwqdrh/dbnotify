package trigger

import (
	"datamanager/pkg/plugger/postgres"
	"fmt"
)

type DefaultTrigger struct {
	postgres.DefaultTriggerPolicy
	logTableName string
	triggerCore  *postgres.TriggerCore
}

func NewDefaultTrigger(db *postgres.PostgresDriver, logTableName string) ITriggerPolicy {
	return &DefaultTrigger{
		logTableName: logTableName,
		triggerCore:  postgres.NewTriggerCore(db.DB),
	}
}

func (t *DefaultTrigger) GetAllTrigger(tableName string) string {
	funcName := tableName + "_auto_log_recored()"
	triggerName := t.GetTriggerName(t, tableName, postgres.ALL)

	return fmt.Sprintf(`
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
		t.logTableName, tableName,
		t.logTableName, tableName,
		t.logTableName, tableName,
		triggerName, tableName, funcName)
}

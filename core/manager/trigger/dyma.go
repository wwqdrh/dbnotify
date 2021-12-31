package trigger

import (
	"datamanager/core/manager/model"
	"datamanager/pkg/plugger/postgres"
	"fmt"

	"gorm.io/gorm"
)

// 动态的表类型 使用raw2json 处理表结构

type (
	DymaTriggerPolicy struct {
		postgres.DefaultTriggerPolicy
		table        interface{} // 目标表
		tableName    string
		logTableName string
		driver       *postgres.PostgresDriver
		outdate      int // 过期天数 用来处理触发器中的惰性删除机制
	}
)

func NewDymaTriggerPolicy(
	db *postgres.PostgresDriver,
	logTableName string,
	table interface{},
	outdate int) postgres.ITriggerPolicy {
	stmt := &gorm.Statement{DB: db.DB}
	stmt.Parse(table)
	tableName := stmt.Schema.Table

	return &DymaTriggerPolicy{
		table:        table,
		tableName:    tableName,
		logTableName: logTableName,
		driver:       db,
		outdate:      outdate,
	}
}

// 为表创建log表字段是固定的
func (p *DymaTriggerPolicy) Initial(this postgres.ITriggerPolicy) error {
	return p.driver.DB.Table(p.logTableName).AutoMigrate(&model.LogTable{})
} // 初始化

func (p *DymaTriggerPolicy) UpdateConfig(conf map[string]interface{}) {
	for key, val := range conf {
		switch key {
		case "outdate":
			p.outdate = val.(int)
		}
	}
}

func (DymaTriggerPolicy) GetPrefix(postgres.ITriggerPolicy) string {
	return "json_log_"
} // 获取前缀

func (p *DymaTriggerPolicy) BeforeUpdateTrigger() string {
	funcName := "auto_log_recored()"
	triggerName := p.GetTriggerName(p, p.tableName, postgres.BEFORE_UPDATE)

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
            DELETE FROM "%s_log" WHERE time < (select now()-interval '%d day');
            INSERT INTO "%s_log" ("log", "action", "time") VALUES (logjson, 'update' , CURRENT_TIMESTAMP);
        END IF;
        IF (TG_OP = 'DELETE') then
        	select json_build_object('data', json_agg(old)) into logjson;
			DELETE FROM "%s_log" WHERE time < (select now()-interval '%d day');
            INSERT INTO "%s_log" ("log", "action", "time") VALUES (logjson, 'delete', CURRENT_TIMESTAMP);
        END IF;
        IF (TG_OP = 'INSERT') then
        	select json_build_object('data', json_agg(new)) into logjson; 
			DELETE FROM "%s_log" WHERE time < (select now()-interval '%d day');
            INSERT INTO "%s_log" ("log", "action", "time") VALUES (logjson, 'insert', CURRENT_TIMESTAMP);
        END IF;
    RETURN NEW;
END$$;

create trigger %s after insert or update or delete on company for each row execute procedure %s;
	`, funcName,
		p.tableName, p.outdate, p.tableName,
		p.tableName, p.outdate, p.tableName,
		p.tableName, p.outdate, p.tableName,
		triggerName, funcName)
}

// 一次性返回所有的trigger
// TODO: 增加truncate的触发器逻辑
func (p *DymaTriggerPolicy) GetAllTrigger(tableName string) string {
	funcName := p.tableName + "_auto_log_recored()"
	triggerName := p.GetTriggerName(p, p.tableName, postgres.BEFORE_UPDATE)

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
            DELETE FROM "%s" WHERE time < (select now()-interval '%d day');
            INSERT INTO "%s" ("table_name", "log", "action", "time") VALUES ('%s', logjson, 'update' , CURRENT_TIMESTAMP);
        END IF;
        IF (TG_OP = 'DELETE') then
        	select json_build_object('data', json_agg(old)) into logjson;
			DELETE FROM "%s" WHERE time < (select now()-interval '%d day');
            INSERT INTO "%s" ("table_name", "log", "action", "time") VALUES ('%s', logjson, 'delete', CURRENT_TIMESTAMP);
        END IF;
        IF (TG_OP = 'INSERT') then
        	select json_build_object('data', json_agg(new)) into logjson; 
			DELETE FROM "%s" WHERE time < (select now()-interval '%d day');
            INSERT INTO "%s" ("table_name", "log", "action", "time") VALUES ('%s', logjson, 'insert', CURRENT_TIMESTAMP);
        END IF;
    RETURN NEW;
END$$;

create trigger %s after insert or update or delete on %s for each row execute procedure %s;
	`, funcName,
		p.logTableName, p.outdate, p.logTableName, p.tableName,
		p.logTableName, p.outdate, p.logTableName, p.tableName,
		p.logTableName, p.outdate, p.logTableName, p.tableName,
		triggerName, p.tableName, funcName)
}

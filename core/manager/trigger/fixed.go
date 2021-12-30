package trigger

import (
	"datamanager/pkg/plugger/postgres"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

type TriggerPolicy struct {
	postgres.DefaultTriggerPolicy
	targetDB     *postgres.PostgresDriver
	table        interface{}
	tableFields  []string
	tablePrimary map[string]bool
	tableName    string
	tableLogName string
	addColumn    []map[string]string // 存储字段
}

func NewTriggerPolicy(db *postgres.PostgresDriver, table interface{}) postgres.ITriggerPolicy {
	stmt := &gorm.Statement{DB: db.DB}
	stmt.Parse(table)
	tableName := stmt.Schema.Table
	tablePrimay := map[string]bool{}
	for _, item := range stmt.Schema.PrimaryFields {
		tablePrimay[item.DBName] = true
	}

	return &TriggerPolicy{
		targetDB:     db,
		table:        table,
		tableFields:  stmt.Schema.DBNames,
		tablePrimary: tablePrimay,
		tableName:    tableName,
		tableLogName: tableName + "_log",
		addColumn: []map[string]string{
			{"name": "action", "type": "varchar(50)"},
			{"name": "createtime", "type": "timestamp"},
		},
	}

}

////////////////////
func (p *TriggerPolicy) Initial(this postgres.ITriggerPolicy) error {
	// 为targetDB上添加新的日志记录表 添加触发器 需要自动创建，删除前删除后 给定名字 类型 字符串数据等等
	// 需要能够动态添加字段 加上action
	if err := p.targetDB.DB.Table(p.tableLogName).AutoMigrate(p.table); err != nil {
		return err
	}

	for _, item := range p.addColumn {
		if err := p.targetDB.AddColumn(p.tableLogName, item["name"], item["type"]); err != nil {
			return err
		}
	}
	return nil
}

// trigger policy
////////////////////
func (p *TriggerPolicy) GetPrefix(this postgres.ITriggerPolicy) string {
	return p.tableName + "_action_log"
}

// BeforeUpdateTrigger 根据表结构创建字段
func (p *TriggerPolicy) BeforeUpdateTrigger() string {
	// 除了字段表 还需要添加新的结构字段 action
	// 添加附件字段
	fields, valueFlag, value := []string{}, []string{}, []string{}
	for _, item := range p.tableFields {
		if _, ok := p.tablePrimary[item]; ok {
			continue
		}
		fields = append(fields, fmt.Sprintf(`"%s"`, item))
		valueFlag = append(valueFlag, "%L")
		value = append(value, fmt.Sprintf(`old.%s`, item))
	}
	for _, item := range p.addColumn {
		switch item["name"] {
		case "action":
			fields = append(fields, `"action"`)
			valueFlag = append(valueFlag, "%L")
			value = append(value, "'before_update'")
		case "createtime":
			fields = append(fields, `"createtime"`)
			valueFlag = append(valueFlag, "%L")
			value = append(value, "current_timestamp")
		}
	}
	fieldsStr, valueFlagStr, valueStr := strings.Join(fields, ","), strings.Join(valueFlag, ","), strings.Join(value, ",")
	funcName := p.GetPrefix(p) + "before_update()"
	triggerName := p.GetTriggerName(p, p.tableName, postgres.BEFORE_UPDATE)

	return fmt.Sprintf(`
	create or replace function %s returns trigger as $$
declare
begin
  EXECUTE format('INSERT INTO %%I.%%I (%s) VALUES (%s)', TG_TABLE_SCHEMA, '%s', %s)
  using new;
  return new;
end
$$ language plpgsql;

create trigger %s before update on %s for each row execute procedure %s;
	`, funcName, fieldsStr, valueFlagStr, p.tableLogName, valueStr, triggerName, p.tableName, funcName)
}

func (p *TriggerPolicy) AfterUpdateTrigger() string {
	fields, valueFlag, value := []string{}, []string{}, []string{}
	for _, item := range p.tableFields {
		if _, ok := p.tablePrimary[item]; ok {
			continue
		}
		fields = append(fields, fmt.Sprintf(`"%s"`, item))
		valueFlag = append(valueFlag, "%L")
		value = append(value, fmt.Sprintf(`new.%s`, item))
	}
	for _, item := range p.addColumn {
		switch item["name"] {
		case "action":
			fields = append(fields, `"action"`)
			valueFlag = append(valueFlag, "%L")
			value = append(value, "'after_update'")
		case "createtime":
			fields = append(fields, `"createtime"`)
			valueFlag = append(valueFlag, "%L")
			value = append(value, "current_timestamp")
		}
	}
	fieldsStr, valueFlagStr, valueStr := strings.Join(fields, ","), strings.Join(valueFlag, ","), strings.Join(value, ",")
	funcName := p.GetPrefix(p) + "after_update()"
	triggerName := p.GetTriggerName(p, p.tableName, postgres.AFTER_UPDATE)

	return fmt.Sprintf(`
	create or replace function %s returns trigger as $$
declare
begin
    EXECUTE format('INSERT INTO %%I.%%I (%s) VALUES (%s)', TG_TABLE_SCHEMA, '%s', %s)
    using new;
    return new;
end
$$ language plpgsql;

create trigger %s after update on %s for each row execute procedure %s;
	`, funcName, fieldsStr, valueFlagStr, p.tableLogName, valueStr, triggerName, p.tableName, funcName)
}

func (p *TriggerPolicy) BeforeDeleteTrigger() string {
	fields, valueFlag, value := []string{}, []string{}, []string{}
	for _, item := range p.tableFields {
		if _, ok := p.tablePrimary[item]; ok {
			continue
		}
		fields = append(fields, fmt.Sprintf(`"%s"`, item))
		valueFlag = append(valueFlag, "%L")
		value = append(value, fmt.Sprintf(`old.%s`, item))
	}
	for _, item := range p.addColumn {
		switch item["name"] {
		case "action":
			fields = append(fields, `"action"`)
			valueFlag = append(valueFlag, "%L")
			value = append(value, "'before_delete'")
		case "createtime":
			fields = append(fields, `"createtime"`)
			valueFlag = append(valueFlag, "%L")
			value = append(value, "current_timestamp")
		}
	}
	fieldsStr, valueFlagStr, valueStr := strings.Join(fields, ","), strings.Join(valueFlag, ","), strings.Join(value, ",")
	funcName := p.GetPrefix(p) + "before_delete()"
	triggerName := p.GetTriggerName(p, p.tableName, postgres.BEFORE_DELETE)

	return fmt.Sprintf(`
	create or replace function %s returns trigger as $$
	declare
	begin
		EXECUTE format('INSERT INTO %%I.%%I (%s) VALUES (%s)'
					, TG_TABLE_SCHEMA, '%s', %s)
		using old;
		return old;
	end
	$$ language plpgsql;

	create trigger %s after delete on %s for each row execute procedure %s;
	`, funcName, fieldsStr, valueFlagStr, p.tableLogName, valueStr, triggerName, p.tableName, funcName)
}

func (p *TriggerPolicy) AfterInsertTrigger() string {
	fields, valueFlag, value := []string{}, []string{}, []string{}
	for _, item := range p.tableFields {
		if _, ok := p.tablePrimary[item]; ok {
			continue
		}
		fields = append(fields, fmt.Sprintf(`"%s"`, item))
		valueFlag = append(valueFlag, "%L")
		value = append(value, fmt.Sprintf(`new.%s`, item))
	}
	for _, item := range p.addColumn {
		switch item["name"] {
		case "action":
			fields = append(fields, `"action"`)
			valueFlag = append(valueFlag, "%L")
			value = append(value, "'after_insert'")
		case "createtime":
			fields = append(fields, `"createtime"`)
			valueFlag = append(valueFlag, "%L")
			value = append(value, "current_timestamp")
		}
	}
	fieldsStr, valueFlagStr, valueStr := strings.Join(fields, ","), strings.Join(valueFlag, ","), strings.Join(value, ",")
	funcName := p.GetPrefix(p) + "after_insert()"
	triggerName := p.GetTriggerName(p, p.tableName, postgres.AFTER_CREATE)

	return fmt.Sprintf(`
	create or replace function %s returns trigger as $$
declare
begin
	EXECUTE format('INSERT INTO %%I.%%I (%s) VALUES (%s)'
                , TG_TABLE_SCHEMA, '%s', %s)
    using new;
	return new;
end
$$ language plpgsql;

create trigger %s after insert on %s for each row execute procedure %s;
`, funcName, fieldsStr, valueFlagStr, p.tableLogName, valueStr, triggerName, p.tableName, funcName)
}

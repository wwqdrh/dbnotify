package datamanager

import (
	"github.com/wwqdrh/datamanager/internal/driver"
	"github.com/wwqdrh/datamanager/runtime"
)

var DefaultHandler = new(BaseStructHandler)

// 默认的转换
const (
	defaultTableSql = `select relname as "table_id", cast(obj_description(relname::regclass, 'pg_class') as varchar) as "table_name"` +
		` from pg_class where relname in (select tablename from pg_tables where schemaname='public') order by relname asc;`

	defaultFieldSql = `select a.attname as "field_id", col_description(a.attrelid,a.attnum) as "field_name"` +
		` FROM pg_class as c, pg_attribute as a WHERE c.relname = ? and a.attrelid = c.oid and a.attnum>0 order by a.attrelid;`
)

type BaseStructHandler struct {
	db *driver.PostgresDriver
}

func (h *BaseStructHandler) GetTables() []*runtime.Table {
	if h.db == nil {
		return nil
	}

	var res []*runtime.Table
	err := h.db.DB.Raw(defaultTableSql).Scan(&res).Error
	if err != nil {
		return nil
	}
	for _, item := range res {
		if item.TableName == "" {
			item.TableName = item.TableID
		}
	}
	return res
}

func (h *BaseStructHandler) GetFields(table string) []*runtime.Fields {
	if h.db == nil {
		return nil
	}

	var res []*runtime.Fields

	tableName := h.db.GetTableName(table)
	err := h.db.DB.Raw(defaultFieldSql, tableName).Scan(&res).Error
	if err != nil {
		return nil
	}
	for _, item := range res {
		if item.FieldName == "" {
			item.FieldName = item.FieldID
		}
	}
	return res
}

func (*BaseStructHandler) GetTableName(tableID string) string {
	return tableID
}

func (*BaseStructHandler) GetFieldName(tableID string, fieldID string) string {
	return fieldID
}

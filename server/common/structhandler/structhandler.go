package structhandler

import (
	"datamanager/server/common/driver"
)

const (
	defaultTableSql = `select relname as "table_id", cast(obj_description(relname::regclass, 'pg_class') as varchar) as "table_name"` +
		` from pg_class where relname in (select tablename from pg_tables where schemaname='public') order by relname asc;`

	defaultFieldSql = `select a.attname as "field_id", col_description(a.attrelid,a.attnum) as "field_name"` +
		` FROM pg_class as c, pg_attribute as a WHERE c.relname = ? and a.attrelid = c.oid and a.attnum>0 order by a.attrelid;`
)

type (
	Table struct {
		TableID   string `json:"table_id"`
		TableName string `json:"table_name"`
		IsListen  bool   `json:"is_listen"`
	}

	Fields struct {
		FieldID   string `json:"field_id"`
		FieldName string `json:"field_name"`
	}

	IStructHandler interface {
		GetTables() []*Table
		GetFields(string) []*Fields
		GetFieldName(string, string) string // 通过表名，字段id 获取字段名
		GetTableName(string) string         // 通过表id获取表名字
	}

	BaseStructHandler struct {
		db *driver.PostgresDriver
	}
)

// 默认的动态字段转换器
func NewBaseStructHandler(db *driver.PostgresDriver) IStructHandler {
	return &BaseStructHandler{
		db: db,
	}
}

func (h *BaseStructHandler) GetTables() []*Table {
	if h.db == nil {
		return nil
	}

	var res []*Table
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

func (h *BaseStructHandler) GetFields(table string) []*Fields {
	if h.db == nil {
		return nil
	}

	var res []*Fields

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

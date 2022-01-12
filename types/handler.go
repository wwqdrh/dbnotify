package types

type (
	Table struct {
		TableID   int    `json:"table_id"`
		TableName string `json:"table_name"`
	}

	Fields struct {
		FieldID   int    `json:"field_id"`
		FieldName string `json:"field_name"`
	}

	IStructHandler interface {
		GetTables() []*Table
		GetFields(string) []*Fields
		GetFieldName(string, string) string // 通过表名，字段id 获取字段名
		GetTableName(string) string         // 通过表id获取表名字
	}
)

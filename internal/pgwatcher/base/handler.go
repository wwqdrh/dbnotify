package base

type (
	IStructHandler interface {
		GetTables() []*Table
		GetFields(string) []*Fields
		GetFieldName(string, string) string // 通过表名，字段id 获取字段名
		GetTableName(string) string         // 通过表id获取表名字
	}

	Table struct {
		TableID   string `json:"table_id"`
		TableName string `json:"table_name"`
		IsListen  bool   `json:"is_listen"`
	}

	Fields struct {
		FieldID   string `json:"field_id"`
		FieldName string `json:"field_name"`
	}
)

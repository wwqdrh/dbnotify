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
	}
)

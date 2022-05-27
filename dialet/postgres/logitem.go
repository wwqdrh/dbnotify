package postgres

import (
	"encoding/json"
	"time"
)

type PostgresLog struct {
	Schema  string                 `json:"schema"`
	Table   string                 `json:"table"`
	Op      int                    `json:"op"`
	Id      string                 `json:"id"`
	Payload map[string]interface{} `json:"payload"`
	Changes map[string]interface{} `json:"changes"`
}

// log unmarshal to struct
// example: {"schema":"public","table":"notes","op":1,"id":"14","payload":{"created_at":null,"id":14,"name":"user1","note":"here is a sample note"}}
func NewPostgresLog(log string) (*PostgresLog, error) {
	var l *PostgresLog
	if err := json.Unmarshal([]byte(log), &l); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *PostgresLog) GetSchema() string {
	return l.Schema
}

func (l *PostgresLog) GetTable() string {
	return l.Table
}

// 获取日志记录类型 ddl dml
func (l *PostgresLog) GetType() string {
	return ""
}

// 具体标签 insert update delete | alter column, table
func (l *PostgresLog) GetLabel() string {
	return "insert"
}

// 获取日志记录时间
func (l *PostgresLog) GetTime() time.Time {
	return time.Now()
}

// 获取具体的负载对象
func (l *PostgresLog) GetPaylod() map[string]interface{} {
	return l.Payload
}

func (l *PostgresLog) GetChange() map[string]interface{} {
	return l.Changes
}

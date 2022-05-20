package postgres

import "time"

type PostgresLog struct{}

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
func (l *PostgresLog) GetPaylod() string {
	return ""
}

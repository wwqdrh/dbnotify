package base

import "time"

type LogTable struct {
	TableName string `json:"table_name"`
	Log       string `json:"log"`
	Action    string
	Time      time.Time `json:"time"`
}

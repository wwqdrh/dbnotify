package logsave

import (
	"time"

	"github.com/wwqdrh/datamanager/internal/datautil"
	"github.com/wwqdrh/datamanager/internal/driver"
)

type (
	TransTable func(string) string
	TransField func(string, string) string
)

type ILogSave interface {
	Write(map[string]interface{}) error
	GetLogger(tableName string, primaryID string, startTime, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error)
	GetAllLog(tableName string, primaryFields []string, start, end *time.Time, page, pageSize int) ([]map[string]interface{}, error)
}

type logPolicy int

const (
	Leveldb logPolicy = iota
)

func NewLogSave(p logPolicy, logdb *driver.LevelDBDriver, queue *datautil.Queue, fntable TransTable, fnfield TransField) ILogSave {
	switch p {
	case Leveldb:
		return &LogSaveLeveldb{
			Queue:   queue,
			LogDB:   logdb,
			FnTable: fntable,
			FnField: fnfield,
		}
	default:
		return nil
	}
}

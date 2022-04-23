package logsave

import (
	"github.com/wwqdrh/datamanager/internal/datautil"
	"github.com/wwqdrh/datamanager/internal/driver"
)

type ILogSave interface {
	Write(map[string]interface{}) error
}

type logPolicy int

const (
	Leveldb logPolicy = iota
)

func NewLogSave(p logPolicy, logdb *driver.LevelDBDriver, queue *datautil.Queue) ILogSave {
	switch p {
	case Leveldb:
		return &LogSaveLeveldb{Queue: queue, LogDB: logdb}
	default:
		return nil
	}
}

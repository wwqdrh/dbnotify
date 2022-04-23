package logsave

import "github.com/wwqdrh/datamanager/internal/datautil"

type ILogSave interface {
	Write(map[string]interface{}) error
}

type logPolicy int

const (
	Leveldb logPolicy = iota
)

func NewLogSave(p logPolicy, queue *datautil.Queue) ILogSave {
	switch p {
	case Leveldb:
		return &LogSaveLeveldb{Queue: queue}
	default:
		return nil
	}
}

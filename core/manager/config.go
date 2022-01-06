package manager

import (
	"datamanager/core/manager/logger"
	"datamanager/core/manager/model"
	"datamanager/core/manager/trigger"
	"datamanager/pkg/plugger/leveldb"
	"datamanager/pkg/plugger/postgres"
	"datamanager/pkg/plugger/sqlite3"
	"fmt"
)

type ManagerConf struct {
	TargetDB      *postgres.PostgresDriver
	VersionDB     *sqlite3.SqliteDriver
	LogDB         *leveldb.LevelDBDriver
	OutDate       int    // 过期时间 单位天
	LogTableName  string // 日志临时表的名字
	LoggerPolicy  logger.ILogger
	TriggerPolicy trigger.ITriggerPolicy
}

func (c *ManagerConf) Init() *ManagerConf {
	if c.LogTableName == "" {
		c.LogTableName = "action_record_log"
	}
	if c.OutDate <= 0 {
		c.OutDate = 15
	}
	if c.LoggerPolicy == nil {
		c.LoggerPolicy = logger.NewConsumerLogger(c.TargetDB, c.LogDB, c.LogTableName, func(s1 string) interface{} {
			if policy, ok := allPolicy.Load(s1); !ok {
				return nil
			} else {
				return policy
			}
		})
	}
	if c.TriggerPolicy == nil {
		c.TriggerPolicy = trigger.NewDefaultTrigger(c.TargetDB, c.LogTableName)
	}

	model.InitRepo(c.LogDB, c.TargetDB, c.LogTableName)

	// 测试有哪些字段
	db, _ := c.LogDB.GetDB("company_log.db")
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		fmt.Printf("[%s]:%s\n", iter.Key(), iter.Value())
	}
	iter.Release()

	return c
}

package model

import (
	"datamanager/pkg/plugger"
)

var (
	PolicyRepo      Policy  = Policy{} // repository
	VersionRepo     Version = Version{}
	LogTableRepo    *LogTableRepository
	LogLocalLogRepo *LocalLog
)

func InitRepo(logDB *plugger.LevelDBDriver, targetDB *plugger.PostgresDriver, logTableName string) {
	LogLocalLogRepo = &LocalLog{
		db: logDB,
	}

	LogTableRepo = &LogTableRepository{
		db:           targetDB.DB,
		logTableName: logTableName,
	}
}

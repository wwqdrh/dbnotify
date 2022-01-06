package model

import (
	"datamanager/pkg/plugger/leveldb"
	"datamanager/pkg/plugger/postgres"
)

var (
	PolicyRepo      Policy  = Policy{} // repository
	VersionRepo     Version = Version{}
	LogTableRepo    *LogTableRepository
	LogLocalLogRepo *LocalLog
)

func InitRepo(logDB *leveldb.LevelDBDriver, targetDB *postgres.PostgresDriver, logTableName string) {
	LogLocalLogRepo = &LocalLog{
		db: logDB,
	}

	LogTableRepo = &LogTableRepository{
		db:           targetDB.DB,
		logTableName: logTableName,
	}
}

package model

import "datamanager/pkg/plugger/leveldb"

var (
	PolicyRepo      Policy  = Policy{} // repository
	VersionRepo     Version = Version{}
	LogTableRepo            = LogTable{}
	LogLocalLogRepo *LocalLog
)

func InitRepo(logDB *leveldb.LevelDBDriver) {
	LogLocalLogRepo = &LocalLog{
		db: logDB,
	}
}

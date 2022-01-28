package dblog

import "datamanager/server/global"

var (
	PolicyRepo       Policy  = Policy{} // repository
	VersionRepo      Version = Version{}
	LogTableRepo     *LogTableRepository
	LogLocalLogRepo  *LocalLog
	LogRepoV2        *LocalLog2
	FieldMappingRepo *fieldMappingRepo
)

func InitRepo(logTableName string) {
	LogLocalLogRepo = &LocalLog{}

	LogTableRepo = &LogTableRepository{
		logTableName: logTableName,
		db:           global.G_DATADB.DB,
	}

	LogRepoV2 = &LocalLog2{}

	FieldMappingRepo = &fieldMappingRepo{}
}

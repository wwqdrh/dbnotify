从postgres中获取日志记录

1、全量触发器
2、触发器+notify

TODO
<!-- 
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
} -->

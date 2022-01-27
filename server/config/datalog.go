package config

type DataLog struct {
	OutDate      int    `mapstructure:"out-date" yaml:"out-date"`             // 过期时间 单位天
	MinLogNum    int    `mapstructure:"min-log-num" yaml:"min-log-num"`       // 最少保留条数
	LogTableName string `mapstructure:"log-table-name" yaml:"log-table-name"` // 日志临时表的名字
}

尝试适配 postgres、mysql等组件

# postgres

1、策略表存储在postgres中，单独建一个表

``` Go
type Policy struct {
	ID            int    `gorm:"PRIMARY_KEY;AUTO_INCREMENT"`
	TableName     string `gorm:"index"` // 数据表的名字
	PrimaryFields string `gorm:"primary_fields"`
	// Fields        string // 需要进行触发的数据 a,b,c,d的格式 先全量存储
	MinLogNum     int // 与outdate一起使用的，即使过期了也要保留最少的记录条数
	Outdate       int // 单位天数 默认为1个月
	// RelaField     string
	Relations     string // `gorm:"description:"[表名].[字段名];[表名].[字段名]...""` 方便多表关联记录的查询
}
```


``` GO
// 如果需要使用中间表转存的话
type LogTable struct {
	ID        uint64      `gorm:"PRIMARY_KEY"`
	TableName string      `gorm:"table_name"`
	Type      string      `gorm:"column:type"`
	Label     string      `gorm:"column:label"`
	Log       driver.JSON `gorm:"TYPE:json"` // ddl: []string; dml: ...
	Action    string
	Time      *time.Time
}
```
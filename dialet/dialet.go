package dialet

import (
	"time"

	"github.com/wwqdrh/datamanager/dialet/postgres"
)

var _ IDialet = &postgres.PostgresDialet{}

type IDialet interface {
	Initial() error          // dialet初始化
	ModifyPolicy() error     // 修改指定数据库数据表的日志存储策略
	ListPolicy() error       // 查看指定数据库的日志策略
	DeletePolicy() error     // 删除某个指定策略
	Watch() chan interface{} // 获取监听channel，能够获取当前的日志修改记录 日志记录格式需要
}

type ILogData interface {
	GetType() string    // 获取日志记录类型 ddl dml
	GetLabel() string   // 具体标签 insert update delete | alter column, table
	GetTime() time.Time // 获取日志记录时间
	GetPaylod() string  // 获取具体的负载对象
}

package pgwatcher

import (
	"github.com/wwqdrh/datamanager/internal/datautil"
	"github.com/wwqdrh/datamanager/internal/driver"
	"github.com/wwqdrh/datamanager/internal/pgwatcher/base"
	"github.com/wwqdrh/datamanager/internal/pgwatcher/policytable"
	"github.com/wwqdrh/datamanager/internal/pgwatcher/policytrigger"
)

type watcherPolicy int

const (
	Table watcherPolicy = iota
	Trigger
)

type IWatcher interface {
	// 初始化
	Initail() error

	GetAllPolicy() []*base.Table
	GetSenseFields(string) []string
	ModifyMinLogNum(tableName string, minLogNum int) error
	ModifyField(tableName string, fields []string) error
	ModifyOutdate(tableName string, outdate int) error
	ModifyPolicy(tableName string, args map[string]interface{}) error

	// 使用不同的策略进行注册
	Register(policy *base.TablePolicy) error
	UnRegister(tableName string) error
	IsRegister(tableName string) bool

	// 所有的表 包括动态创建的表
	ListenAll() chan interface{}

	// 以table粒度进行监听
	ListenTable(tableName string) chan interface{}
} // 不同的策略

func NewWatcherPolicy(
	p watcherPolicy,
	db *driver.PostgresDriver,
	handler base.IStructHandler,
	Outdate int,
	MinLogNum int,
	TempLogTable string,
	PerReadNum int,
	queue *datautil.Queue,
) IWatcher {
	switch p {
	case Table:
		return &policytable.PgwatcherTable{
			DB:           db,
			Outdate:      Outdate,
			MinLogNum:    MinLogNum,
			TempLogTable: TempLogTable,
			PerReadNum:   PerReadNum,
			Handler:      handler,
			Readch:       queue,
		}
	case Trigger:
		return &policytrigger.PgWatcherNotify{DB: db}
	default:
		return nil
	}
}

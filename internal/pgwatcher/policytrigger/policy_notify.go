package policytrigger

import (
	"github.com/wwqdrh/datamanager/internal/driver"
	"github.com/wwqdrh/datamanager/internal/pgwatcher/base"
)

type PgWatcherNotify struct {
	DB *driver.PostgresDriver
}

func (p *PgWatcherNotify) Initail() error {
	return nil
}

// 使用不同的策略进行注册
// 创建触发器
func (p *PgWatcherNotify) Register(policy *base.TablePolicy) error {
	panic("not implemented") // TODO: Implement
}
func (p *PgWatcherNotify) IsRegister(tableName string) bool {
	return false
}
func (p *PgWatcherNotify) GetSenseFields(tableName string) []string {
	return nil
}
func (p *PgWatcherNotify) UnRegister(tableName string) error {
	return nil
}
func (p *PgWatcherNotify) GetAllPolicy() []*base.Table {
	return nil
}

// 所有的表 包括动态创建的表
func (p *PgWatcherNotify) ListenAll() chan interface{} {
	panic("not implemented") // TODO: Implement
}

// 以table粒度进行监听
func (p *PgWatcherNotify) ListenTable(tableName string) chan interface{} {
	panic("not implemented") // TODO: Implement
}

func (p *PgWatcherNotify) ModifyMinLogNum(tableName string, minLogNum int) error { return nil }
func (p *PgWatcherNotify) ModifyField(tableName string, fields []string) error   { return nil }
func (p *PgWatcherNotify) ModifyOutdate(tableName string, outdate int) error     { return nil }
func (p *PgWatcherNotify) ModifyPolicy(tableName string, args map[string]interface{}) error {
	return nil
}

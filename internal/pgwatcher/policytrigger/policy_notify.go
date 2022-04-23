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

// 所有的表 包括动态创建的表
func (p *PgWatcherNotify) ListenAll() chan interface{} {
	panic("not implemented") // TODO: Implement
}

// 以table粒度进行监听
func (p *PgWatcherNotify) ListenTable(tableName string) chan interface{} {
	panic("not implemented") // TODO: Implement
}

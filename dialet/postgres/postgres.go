package postgres

import "gorm.io/gorm"

type PostgresDialet struct {
	db *gorm.DB
}

func NewPostgresDialet(db *gorm.DB) *PostgresDialet {
	return &PostgresDialet{
		db: db,
	}
}

// dialet初始化
func (p *PostgresDialet) Initial() error {
	return nil
}

// 修改指定数据库数据表的日志存储策略
func (p *PostgresDialet) ModifyPolicy() error {
	return nil
}

// 查看指定数据库的日志策略
func (p *PostgresDialet) ListPolicy() error {
	return nil
}

// 删除某个指定策略
func (p *PostgresDialet) DeletePolicy() error {
	return nil
}

// 获取监听channel，能够获取当前的日志修改记录 日志记录格式需要
func (p *PostgresDialet) Watch() chan interface{} {
	ch := make(chan interface{}, 100)
	for i := 0; i < 10; i++ {
		ch <- new(PostgresLog)
	}
	return ch
}

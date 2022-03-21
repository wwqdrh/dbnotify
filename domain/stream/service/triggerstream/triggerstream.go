package triggerstream

import (
	"context"
	"errors"
	"time"

	"github.com/wwqdrh/datamanager/domain/stream/entity"
	"github.com/wwqdrh/datamanager/domain/stream/repository"
	"github.com/wwqdrh/datamanager/domain/stream/service/base"
)

// 通过触发器全量记录所有的操作

type ITriggerStream interface {
	base.IStreamInitial
	base.IStreamPolicy
}

type triggerStream struct {
	ch  chan<- map[string]interface{}
	ctx context.Context
}

func NewTriggerStream() ITriggerStream {
	return &triggerStream{}
}

func (s *triggerStream) Install(ctx context.Context) (chan map[string]interface{}, error) {
	ch := make(chan map[string]interface{}, 10)
	s.ch = ch
	s.ctx = ctx
	return ch, nil
} // 执行初始化问题, 返回通道用于exporter来获取数据并使用

func (s *triggerStream) Start(tableName string, id uint64, num int) error {
	for {
		select {
		case <-s.ctx.Done():
			return errors.New("退出")
		default:
			data, err := repository.LogTableRepo.ReadBytableNameAndLimit(tableName, id, num)
			if err == nil {
				d := map[string]interface{}{
					"datar": data,
				}
				s.ch <- d
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func (s *triggerStream) GetAllPolicy() map[string]*entity.Policy {
	return nil
} // 获取所有的表格当前策略

func (s *triggerStream) ModifyOutdate(tableName string, outdate int) error {
	return nil
} // 修改表格的过期时间

func (s *triggerStream) ModifyField(tableName string, fields []string) error {
	return nil
} // 修改监听字段

func (s *triggerStream) ModifyMinLogNum(tableName string, minLogNum int) error {
	return nil
}

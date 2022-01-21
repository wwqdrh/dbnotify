package datamanager

import (
	"time"

	"datamanager/server/common"
	"datamanager/server/manager"
	"datamanager/server/manager/model"
	"datamanager/server/pkg/datautil"

	"datamanager/server/base"
)

const (
	nums int = 2000 // 每次执行2000行的数据
)

type LogLoadRunner struct {
	base.Runner
	queue *datautil.Queue
	fn    func(tableName string, id uint64, num int) ([]*model.LogTable, error)
}

func NewLogLoadRunner(queue *datautil.Queue) *LogLoadRunner {
	return &LogLoadRunner{
		queue: queue,
		fn:    common.Mana.LoggerPolicy.Load(),
	}
}

// 每隔一段时间读取n条数据加入到任务队列中
func (r *LogLoadRunner) Run() {
	var id uint64

	for {
		if !initDB {
			r.Sleep(1 * time.Second)
			continue
		}
		manager.GetAllPolicy().Range(func(key, value interface{}) bool {
			tableName := key.(string)
			policy := value.(*model.Policy)
			res, err := r.fn(tableName, id, nums)
			if err != nil {
				r.Warn("获取数据失败", err.Error())
				return true
			}
			if len(res) == 0 {
				return true
			}

			if !<-r.queue.Push(map[string]interface{}{
				"data":   res,
				"policy": policy,
			}) {
				return true
			}
			if err := model.LogTableRepo.DeleteByID(res); err != nil {
				r.Warn("删除数据失败:", err.Error())
				return true
			}

			if res[len(res)-1].ID > id {
				id = res[len(res)-1].ID
			}
			return true
		})
		r.Sleep(time.Minute * 10)
	}
}

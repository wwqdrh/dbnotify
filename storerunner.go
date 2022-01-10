package datamanager

import (
	"time"

	"datamanager/base"
	"datamanager/common"
	"datamanager/pkg/datautil"

	"datamanager/manager/model"
)

// 从任务队列中获取任务 需要分组处理 然后当整个处理完了之后进行通知，然后生产者再去删除数据库中的数据

type LogStoreRunner struct {
	base.Runner
	queue *datautil.Queue
	fn    func(policy *model.Policy, data []*model.LogTable) error
}

func newLogStoreRunner(queue *datautil.Queue, num int) []base.IRunner {
	rs := make([]base.IRunner, num)

	for i := 0; i < num; i++ {
		rs[i] = &LogStoreRunner{
			queue: queue,
			fn:    common.Mana.LoggerPolicy.Store(),
		}
	}

	return rs
}

func (r *LogStoreRunner) Run() {
	for {
		if !initDB {
			r.Sleep(1 * time.Second)
			continue
		}

		if task := r.queue.GetTask(); task != nil {
			payload := task.PayLoad.(map[string]interface{}) // data, policy
			data := payload["data"].([]*model.LogTable)
			policy := payload["policy"].(*model.Policy)
			if err := r.fn(policy, data); err != nil {
				r.Warn("消费者执行错误", err.Error())
				task.SetResult(false)
			} else {
				task.SetResult(true)
			}
		}

		r.Sleep(1 * time.Minute)
	}
}

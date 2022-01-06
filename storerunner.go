package datamanager

import (
	"datamanager/base"
	"datamanager/common"
	"datamanager/pkg/datautil"
	"time"

	"datamanager/core/manager/model"
)

// 从任务队列中获取任务 需要分组处理 然后当整个处理完了之后进行通知，然后生产者再去删除数据库中的数据

type LogStoreRunner struct {
	base.Runner
	queue *datautil.Queue
	fn    func(policy *model.Policy, data []*model.LogTable) error
}

func newLogStoreRunner(queue *datautil.Queue) *LogStoreRunner {
	return &LogStoreRunner{
		queue: queue,
		fn:    common.Mana.LoggerPolicy.Store(),
	}
}

func (r *LogStoreRunner) Run() {
	for {
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
		r.Sleep(1 * time.Minute) // 等待一分钟
	}
}

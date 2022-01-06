package datautil

import (
	"sync"
	"sync/atomic"
)

// 生产消费者模型 每个任务需要有ack处理机制

type (
	task struct {
		ID      int64       // 任务的唯一id
		PayLoad interface{} // 任务负载信息，通知消费者处理任务时有哪些数据
		Ack     chan bool   // 消费者是否处理完成
		q       *Queue      // 指向所在的任务队列
		done    bool        // 是否执行完了
	}

	Queue struct {
		data []*task // 任务队列
		r    sync.RWMutex
	}
)

var globalID int64 = 0

func getglobalID() int64 {
	return atomic.AddInt64(&globalID, 1)
}

func NewQueue() *Queue {
	return &Queue{
		data: []*task{},
		r:    sync.RWMutex{},
	}
}

func (q *Queue) Length() int {
	q.r.RLock()
	defer q.r.RUnlock()
	return len(q.data)
}

// 添加任务
func (q *Queue) Push(data interface{}) chan bool {
	q.r.Lock()
	defer q.r.Unlock()
	ack := make(chan bool, 1)
	q.data = append(q.data, &task{
		ID:      getglobalID(),
		PayLoad: data,
		Ack:     ack,
		q:       q,
	})
	return ack
}

func (q *Queue) GetTask() *task {
	q.r.Lock()
	defer q.r.Unlock()
	if len(q.data) == 0 {
		return nil
	}
	res := q.data[0]
	q.data = q.data[1:len(q.data)]
	return res
}

////////////////////
// task
////////////////////

func (t *task) SetResult(result bool) {
	if t.done {
		return
	}
	if result {
		t.done = true
		t.Ack <- true
	} else {
		t.q.Push(t) // 重新加入到任务队列中
		t.Ack <- false
	}
}

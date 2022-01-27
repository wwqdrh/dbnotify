package datautil

import (
	"testing"
)

func TestConsumer(t *testing.T) {
	queue := NewQueue()

	// 添加任务
	ack1 := queue.Push("任务1")
	ack2 := queue.Push("任务2")
	ack3 := queue.Push("任务3")

	// 执行任务
	task1 := queue.GetTask()
	task2 := queue.GetTask()
	task3 := queue.GetTask()
	t.Log(task1.ID, task1.PayLoad)
	task1.SetResult(true)
	t.Log(task2.ID, task2.PayLoad)
	task2.SetResult(true)
	t.Log(task3.ID, task3.PayLoad)
	task3.SetResult(false)
	t.Log(false)

	// 查看结果
	if !<-ack1 {
		t.Error("task1执行失败")
	}
	if !<-ack2 {
		t.Error("task2执行失败")
	}
	if <-ack3 {
		t.Error("task3执行失败")
	}

	// 判断是否还有一个任务在队列中
	if queue.Length() != 1 {
		t.Error("任务数量不对")
	}
}

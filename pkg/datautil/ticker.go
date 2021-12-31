package datautil

import (
	"fmt"
	"time"
)

// 定义函数类型
type Fn func() error

// 定时器中的成员
type MyTicker struct {
	MyTick *time.Ticker
	Runner Fn
}

func NewMyTick(interval time.Duration, f Fn) *MyTicker {
	return &MyTicker{
		MyTick: time.NewTicker(interval),
		Runner: f,
	}
}

// 启动定时器需要执行的任务
func (t *MyTicker) Start() {
	if err := t.Runner(); err != nil {
		fmt.Println(" 定时任务执行退出", t.Runner)
		return
	}

	for range t.MyTick.C {
		if err := t.Runner(); err != nil {
			fmt.Println(" 定时任务执行退出", t.Runner)
			return
		}
	}
}

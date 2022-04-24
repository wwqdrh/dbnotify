package datamanager

import "errors"

// 注册方法
func RegisterTable(policy *TablePolicy) error {
	watcher := R.GetWatcher()
	if watcher == nil {
		return errors.New("为执行初始化")
	}

	return watcher.Register(policy)
}

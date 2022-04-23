package runtime

import (
	"github.com/wwqdrh/datamanager/internal/datautil"
	"github.com/wwqdrh/datamanager/internal/driver"
	"github.com/wwqdrh/datamanager/internal/logsave"
	"github.com/wwqdrh/datamanager/internal/pgwatcher"
)

var Runtime IRuntime = new(runtime)

type IRuntime interface {
	GetDB() *driver.PostgresDriver
	SetDB(func() (*driver.PostgresDriver, error)) error

	GetLogDB() *driver.LevelDBDriver
	SetLogDB(func() (*driver.LevelDBDriver, error)) error

	GetBackQueue() *datautil.Queue
	SetBackQueue(func() (*datautil.Queue, error)) error

	// 表字段的转换
	GetFieldHandler() IStructHandler
	SetFieldHandler(func() (IStructHandler, error)) error

	// 变量
	GetConfig() *RuntimeConfig
	SetConfig(func() (*RuntimeConfig, error)) error

	// pgwatcher
	GetWatcher() pgwatcher.IWatcher
	SetWatcher(func() (pgwatcher.IWatcher, error)) error

	// save
	GetLogSave() logsave.ILogSave
	SetLogSave(func() (logsave.ILogSave, error)) error
}

type runtime struct {
	logdb   *driver.LevelDBDriver
	datadb  *driver.PostgresDriver
	queue   *datautil.Queue
	config  *RuntimeConfig
	watcher pgwatcher.IWatcher
	saver   logsave.ILogSave
	handler IStructHandler
}

func (r *runtime) GetDB() *driver.PostgresDriver {
	return r.datadb
}

func (r *runtime) SetDB(fn func() (*driver.PostgresDriver, error)) error {
	db, err := fn()
	if err != nil {
		return err
	}
	r.datadb = db
	return nil
}

func (r *runtime) GetLogDB() *driver.LevelDBDriver {
	return r.logdb
}

func (r *runtime) SetLogDB(fn func() (*driver.LevelDBDriver, error)) error {
	db, err := fn()
	if err != nil {
		return err
	}
	r.logdb = db
	return nil
}

func (r *runtime) GetBackQueue() *datautil.Queue {
	return r.queue
}

func (r *runtime) SetBackQueue(fn func() (*datautil.Queue, error)) error {
	db, err := fn()
	if err != nil {
		return err
	}
	r.queue = db
	return nil
}

// 表字段的转换
func (r *runtime) GetFieldHandler() IStructHandler {
	return r.handler
}

func (r *runtime) SetFieldHandler(fn func() (IStructHandler, error)) error {
	c, err := fn()
	if err != nil {
		return err
	}
	r.handler = c
	return nil
}

// 变量
func (r *runtime) GetConfig() *RuntimeConfig {
	return r.config
}

func (r *runtime) SetConfig(conf func() (*RuntimeConfig, error)) error {
	c, err := conf()
	if err != nil {
		return err
	}
	r.config = c
	return nil
}

// pgwatcher
func (r *runtime) GetWatcher() pgwatcher.IWatcher {
	return r.watcher
}

func (r *runtime) SetWatcher(fn func() (pgwatcher.IWatcher, error)) error {
	c, err := fn()
	if err != nil {
		return err
	}
	r.watcher = c
	return nil
}

// save
func (r *runtime) GetLogSave() logsave.ILogSave {
	return r.saver
}

func (r *runtime) SetLogSave(fn func() (logsave.ILogSave, error)) error {
	c, err := fn()
	if err != nil {
		return err
	}
	r.saver = c
	return nil
}

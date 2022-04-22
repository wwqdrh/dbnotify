package runtime

import (
	"github.com/wwqdrh/datamanager/internal/datautil"
	"github.com/wwqdrh/datamanager/internal/driver"
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
	GetFieldHandler() interface{}
	SetFieldHandler(func() (interface{}, error)) error

	// 变量
	GetConfig() *RuntimeConfig
	SetConfig(func() (*RuntimeConfig, error)) error
}

type runtime struct {
	logdb  *driver.LevelDBDriver
	datadb *driver.PostgresDriver
	queue  *datautil.Queue
	config *RuntimeConfig
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
func (r *runtime) GetFieldHandler() interface{} {
	panic("not implemented") // TODO: Implement
}

func (r *runtime) SetFieldHandler(_ func() (interface{}, error)) error {
	panic("not implemented") // TODO: Implement
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

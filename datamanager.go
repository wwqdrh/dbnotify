package datamanager

import (
	"datamanager/base"
	"datamanager/common"
	"datamanager/pkg/datautil"
	"datamanager/pkg/plugger/postgres"
	"errors"
)

type (
	dataManagerService struct {
		base.Service
	}
)

var (
	taskQueue *datautil.Queue
)

// Register 进行注册
func Register(config *postgres.PostgresConfig, tables ...interface{}) []error {
	// 除了注册表 还需要启动服务
	if err := common.InitApp(config.Init()); len(err) != 0 {
		return err
	}

	errs := []error{}
	for _, item := range tables {
		if err := common.AddTable(item); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// 添加backend接口运行
func InitService(config *postgres.PostgresConfig, tables ...interface{}) error {
	service, err := NewDataManagerService(config, tables...)
	if len(err) > 0 {
		return errors.New("datamanager初始化失败")
	}
	base.InitService(service, NewLogLoadRunner(taskQueue), newLogStoreRunner(taskQueue))
	return nil
}

func NewDataManagerService(config *postgres.PostgresConfig, tables ...interface{}) (*dataManagerService, []error) {
	// 除了注册表 还需要启动服务
	if err := common.InitApp(config.Init()); len(err) != 0 {
		return nil, err
	}
	errs := []error{}
	for _, item := range tables {
		if err := common.AddTable(item); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return nil, errs
	}

	// 创建任务队列
	taskQueue = datautil.NewQueue()

	// 返回service1
	return &dataManagerService{}, nil
}

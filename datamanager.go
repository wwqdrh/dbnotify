package datamanager

import (
	"datamanager/common"
	"datamanager/pkg/plugger/postgres"
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

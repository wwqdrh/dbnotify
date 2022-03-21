package datamanager

import (
	"context"

	"github.com/wwqdrh/datamanager/core"
	"github.com/wwqdrh/datamanager/internal/structhandler"
	"github.com/wwqdrh/datamanager/pkg"
	"github.com/wwqdrh/datamanager/service"

	"gorm.io/gorm"
)

// Register 初始化db等 调用时的入口函数
func Register(db *gorm.DB, structHandler structhandler.IStructHandler, tables ...pkg.TablePolicy) error {
	core.Init(
		core.InitConfig(),
		core.InitDataDB(db),
		core.InitLogDB(),
		core.InitTaskQueue(),
		core.InitStructHandler(structHandler),
		core.InitService(
			service.InitService,
			func() { service.InitPolicyService("trigger", "leveldb") },
		),
	)

	if err := service.PolicyService.Initial(
		tables...,
	); err != nil {
		return err
	}

	return nil
}

// AddTable 动态表
func AddTable(table interface{}, fields []string, ignore []string) error {
	// service.MetaService.AddTable(table, fields, ignore)
	return service.PolicyService.RegisterTable(table, 10, 10, fields, ignore)
}

// Start 开启操作日志监听
func Start(ctx context.Context) {
	go service.PolicyService.LoadFactory()(ctx)
	// 1个loader线程

	// 10个writer线程
	for i := 0; i < 10; i++ {
		go service.PolicyService.WriteFactory()(ctx)
	}
}

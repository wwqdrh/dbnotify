package datamanager

import (
	"context"

	"gorm.io/gorm"

	"github.com/ohko/hst"
	"github.com/wwqdrh/datamanager/app"
	"github.com/wwqdrh/datamanager/app/service"
	"github.com/wwqdrh/datamanager/core"
	"github.com/wwqdrh/datamanager/domain/exporter"
	"github.com/wwqdrh/datamanager/domain/stream"
	stream_vo "github.com/wwqdrh/datamanager/domain/stream/vo"
	"github.com/wwqdrh/datamanager/internal/structhandler"
)

type TablePolicy = stream_vo.TablePolicy

type StructHandler = stream_vo.IStructHandler

func init() {
	core.Init(
		core.InitConfig(),
	)
}

// 加载路由 提供可视化界面
func RegisterView(httpHandler *hst.HST) *hst.HST {
	return app.RegisterApi(httpHandler)
}

// Register 初始化db等 调用时的入口函数
func RegisterStream(db *gorm.DB, structHandler structhandler.IStructHandler, tables ...TablePolicy) error {
	core.Init(
		core.InitLogDB(),
		core.InitTaskQueue(),
		core.InitDataDB(db),
		core.InitStructHandler(structHandler),
		core.InitDomain(
			exporter.Register,
			stream.Register,
		),
	)

	// TODO: metaserver未注册 移到
	if err := new(service.MetaService).InitApp(
		// if err := service.NewPolicyService().Initial(
		tables...,
	); err != nil {
		return err
	}

	return nil
}

// AddTable 动态表
func AddTable(table interface{}, fields []string, ignore []string) error {
	// service.MetaService.AddTable(table, fields, ignore)
	return service.NewPolicyService().RegisterTable(table, 10, 10, fields, ignore)
}

// Start 开启操作日志监听
func StartBackground(ctx context.Context) {
	// 1个loader线程
	go service.NewPolicyService().LoadFactory()(ctx)
	// 10个writer线程
	for i := 0; i < 10; i++ {
		go service.NewPolicyService().WriteFactory()(ctx)
	}
}

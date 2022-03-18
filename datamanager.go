package datamanager

import (
	"errors"
	"fmt"

	"github.com/wwqdrh/datamanager/core"
	stream_service "github.com/wwqdrh/datamanager/domain/stream/service/fulltrigger"
	"github.com/wwqdrh/datamanager/internal/structhandler"
	"github.com/wwqdrh/datamanager/pkg"
	"github.com/wwqdrh/datamanager/service"

	"gorm.io/gorm"
)

// Register 初始化db等 调用时的入口函数
func Register(db *gorm.DB, structHandler structhandler.IStructHandler, tables ...*pkg.TablePolicy) error {
	core.Init(
		core.InitConfig(),
		core.InitDataDB(db),
		core.InitLogDB(),
		core.InitTaskQueue(),
		core.InitStructHandler(structHandler),
		core.InitService(service.InitService),
	)

	errs := service.MetaService.InitApp(tables...)
	if len(errs) > 0 {
		for _, item := range errs {
			fmt.Println(item.Error())
		}
		return errors.New("初始化失败")
	}

	return nil
}

// AddTable 动态表
func AddTable(table interface{}, fields []string, ignore []string) {
	service.MetaService.AddTable(table, fields, ignore)
}

// Start 开启操作日志监听
func Start() {
	stream_service.StartFullTrigger()
}

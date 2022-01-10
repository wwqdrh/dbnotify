package datamanager

import (
	"errors"
	"fmt"

	"datamanager/base"
	"datamanager/common"
	"datamanager/pkg/datautil"
	"datamanager/types"

	"gorm.io/gorm"
)

type (
	dataManagerService struct {
		base.Service
	}

	BaseStructHandler struct {
	}
)

var (
	taskQueue *datautil.Queue
	initDB    bool // 是否初始化了数据库
	Handler   types.IStructHandler
)

// InitService 初始化后台服务 可以空跑 之后再初始化数据库连接
func InitService() {
	// 创建任务队列
	taskQueue = datautil.NewQueue()
	runners := []base.IRunner{
		NewLogLoadRunner(taskQueue),
	}
	runners = append(runners, newLogStoreRunner(taskQueue, 10)...)
	base.InitService(&dataManagerService{}, runners...)
}

// InitDB 初始化db 这里的表是强绑定的
// structHandler 回调接口 提供获取表列表 表名字的回调
func InitDB(db *gorm.DB, logPath string, structHandler types.IStructHandler, tables ...interface{}) error {
	if structHandler == nil {
		structHandler = new(BaseStructHandler)
	}

	errs := common.InitApp(db, logPath, structHandler, tables...)
	if len(errs) > 0 {
		for _, item := range errs {
			fmt.Println(item.Error())
		}
		return errors.New("初始化失败")
	}

	initDB = true
	return nil
}

func (*BaseStructHandler) GetTables() []*types.Table {
	if common.PostgresDriver == nil {
		return nil
	}
	res := []*types.Table{}
	tables := common.PostgresDriver.ListTable()
	for _, item := range tables {
		res = append(res, &types.Table{
			TableID:   -1,
			TableName: item,
		})
	}
	return res
}

func (*BaseStructHandler) GetFields(tableName string) []*types.Fields {
	if common.PostgresDriver == nil {
		return nil
	}

	res := []*types.Fields{}
	fields := common.PostgresDriver.ListTableField(tableName)
	for _, field := range fields {
		res = append(res, &types.Fields{
			FieldID:   -1,
			FieldName: field,
		})
	}
	return res
}

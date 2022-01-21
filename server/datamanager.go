package datamanager

import (
	"errors"
	"fmt"

	"datamanager/server/common"
	"datamanager/server/pkg/datautil"
	"datamanager/server/types"

	"datamanager/server/base"

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

func AddTable(table interface{}, fields []string, ignore []string) {
	common.AddTable(table, fields, ignore)
}

func (*BaseStructHandler) GetTables() []*types.Table {
	if common.PostgresDriver == nil {
		return nil
	}
	tables := common.PostgresDriver.ListTable()
	for _, item := range tables {
		if item.TableName == "" {
			item.TableName = item.TableID
		}
	}
	return tables
}

func (*BaseStructHandler) GetFields(tableName string) []*types.Fields {
	if common.PostgresDriver == nil {
		return nil
	}

	fields := common.PostgresDriver.ListTableField(tableName)
	for _, field := range fields {
		if field.FieldName == "" {
			field.FieldName = field.FieldID
		}
	}
	return fields
}

func (*BaseStructHandler) GetTableName(tableID string) string {
	return tableID
}

func (*BaseStructHandler) GetFieldName(tableID string, fieldID string) string {
	return fieldID
}

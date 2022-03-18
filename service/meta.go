package service

import (
	"time"

	"github.com/wwqdrh/datamanager/core"
	stream_entity "github.com/wwqdrh/datamanager/domain/stream/entity"
	stream_service "github.com/wwqdrh/datamanager/domain/stream/service/fulltrigger"
	"github.com/wwqdrh/datamanager/internal/structhandler"
	"github.com/wwqdrh/datamanager/pkg"
)

var MetaService = &metaService{}

type metaService struct {
}

// 初始化db的触发器等
func (s *metaService) InitialDB() error {
	return stream_service.MetaService.InitialDB()
}

// InitApp 读取表的策略
func (s *metaService) InitApp(tables ...*pkg.TablePolicy) (errs []error) {
	return stream_service.MetaService.InitApp(tables...)
}

// AddTable 添加动态变
func (s *metaService) AddTable(table interface{}, senseFields []string, ignoreFields []string) {
	stream_service.MetaService.AddTable(table, senseFields, ignoreFields)
}

func (s *metaService) Register(table interface{}, min_log_num, outdate int, fields []string, ignoreFields []string) error {
	return stream_service.MetaService.Register(table, min_log_num, outdate, fields, ignoreFields)
}

func (s *metaService) ListTable2() []*structhandler.Table {
	result := core.G_StructHandler.GetTables()

	tableMapping := map[string]*structhandler.Table{}
	for _, item := range result {
		tableMapping[item.TableName] = item
	}
	stream_service.MetaService.AllPolicy.Range(func(key, value interface{}) bool {
		item := value.(*stream_entity.Policy)
		if val, ok := tableMapping[item.TableName]; ok {
			val.IsListen = true
		}
		return true
	})

	return result
}

func (s *metaService) UnRegister(table string) error {
	return stream_service.MetaService.UnRegister(table)
}

func (s *metaService) ListTableAllLog(tableName string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	return stream_service.MetaService.ListTableAllLog(tableName, startTime, endTime, page, pageSize)
}

func (s *metaService) ListTableLog(tableName string, recordID string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	return stream_service.MetaService.ListTableLog(tableName, recordID, startTime, endTime, page, pageSize)
}

func (s *metaService) ModifyPolicy(tableName string, args map[string]interface{}) error {
	return stream_service.MetaService.ModifyPolicy(tableName, args)
}

func (s *metaService) ListTableField(tableName string) []*structhandler.Fields {
	return stream_service.MetaService.ListTableField(tableName)
}

package service

import (
	"errors"
	"time"

	"github.com/wwqdrh/datamanager/core"
	stream_entity "github.com/wwqdrh/datamanager/domain/stream/entity"
	stream_service "github.com/wwqdrh/datamanager/domain/stream/service/fulltrigger"
	"github.com/wwqdrh/datamanager/domain/stream/vo"
	"github.com/wwqdrh/datamanager/internal/structhandler"
)

////////////////////
// 基础信息
////////////////////

type MetaService struct {
}

// 初始化db的触发器等
func (s *MetaService) InitialDB() error {
	return stream_service.DefaultMetaService().InitialDB()
}

// InitApp 读取表的策略
func (s *MetaService) InitApp(tables ...vo.TablePolicy) error {
	errs := stream_service.DefaultMetaService().InitApp(tables...)
	if len(errs) > 0 {
		return errors.New("注册失败")
	}
	return nil
}

// AddTable 添加动态变
func (s *MetaService) AddTable(table interface{}, senseFields []string, ignoreFields []string) {
	stream_service.DefaultMetaService().AddTable(table, senseFields, ignoreFields)
}

func (s *MetaService) Register(table interface{}, min_log_num, outdate int, fields []string, ignoreFields []string) error {
	return stream_service.DefaultMetaService().Register(table, min_log_num, outdate, fields, ignoreFields)
}

func (s *MetaService) ListTable2() []*structhandler.Table {
	result := core.G_StructHandler.GetTables()

	tableMapping := map[string]*structhandler.Table{}
	for _, item := range result {
		tableMapping[item.TableName] = item
	}
	stream_service.DefaultMetaService().AllPolicy.Range(func(key, value interface{}) bool {
		item := value.(*stream_entity.Policy)
		if val, ok := tableMapping[item.TableName]; ok {
			val.IsListen = true
		}
		return true
	})

	return result
}

func (s *MetaService) UnRegister(table string) error {
	return stream_service.DefaultMetaService().UnRegister(table)
}

func (s *MetaService) ListTableAllLog(tableName string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	return stream_service.DefaultMetaService().ListTableAllLog(tableName, startTime, endTime, page, pageSize)
}

func (s *MetaService) ListTableLog(tableName string, recordID string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	return stream_service.DefaultMetaService().ListTableLog(tableName, recordID, startTime, endTime, page, pageSize)
}

func (s *MetaService) ModifyPolicy(tableName string, args map[string]interface{}) error {
	return stream_service.DefaultMetaService().ModifyPolicy(tableName, args)
}

func (s *MetaService) ListTableField(tableName string) []*structhandler.Fields {
	return stream_service.DefaultMetaService().ListTableField(tableName)
}

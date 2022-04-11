package service

import (
	"context"
	"time"

	"github.com/wwqdrh/datamanager/core"
	exporter_base "github.com/wwqdrh/datamanager/domain/exporter/service/base"
	"github.com/wwqdrh/datamanager/domain/exporter/service/leveldblog"
	stream_base "github.com/wwqdrh/datamanager/domain/stream/service/base"
	"github.com/wwqdrh/datamanager/domain/stream/service/triggerstream"
	"github.com/wwqdrh/datamanager/domain/stream/vo"
)

// 传递各类策略

type PolicyService struct {
	stream   stream_base.ITriggerStream
	exporter exporter_base.IExporter
}

func NewPolicyService() *PolicyService {
	return (&PolicyService{}).Init()
}

func (s *PolicyService) Init() *PolicyService {
	switch core.G_CONFIG.DataLog.StreamPolicy {
	default:
		s.stream = triggerstream.NewTriggerStream()
	}

	switch core.G_CONFIG.DataLog.ExportPolicy {
	default:
		s.exporter = leveldblog.NewLeveldbLog()
	}

	return s
}

// 初始化stream以及exporter
func (s *PolicyService) Initial(tables ...vo.TablePolicy) error {
	ch, err := s.stream.Install(tables...)
	if err != nil {
		return err
	}

	// 接收值的通道以及字段转换的结构体
	if err := s.exporter.Install(ch, nil); err != nil {
		return err
	}

	return nil
}

func (s *PolicyService) RegisterTable(table interface{}, min_log_num, outdate int, fields []string, ignoreFields []string) error {
	return s.stream.AddPolicy(vo.TablePolicy{
		Table:        table,
		MinLogNum:    min_log_num,
		Outdate:      outdate,
		SenseFields:  fields,
		IgnoreFields: ignoreFields,
	})
}

func (s *PolicyService) RemoveTable(table string) error {
	return s.stream.RemovePolicy(table)
}

func (s *PolicyService) ListTableAllLog(tableName string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	return s.exporter.GetAllLogger(tableName, startTime, endTime, page, pageSize)
}

func (s *PolicyService) ListTableByTime(tableName string, recordID string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	return s.exporter.GetLoggerByID(tableName, recordID, startTime, endTime, page, pageSize)
}

func (s *PolicyService) ModifyPolicy(tableName string, args map[string]interface{}) error {
	return s.stream.ModifyPolicy(tableName, args)
}

func (s *PolicyService) ListTableField(tableName string) []*vo.Fields {
	return s.stream.ListTableField(tableName)
}

// 返回读取操作的 构造函数
func (s *PolicyService) LoadFactory() func(context.Context) error {
	return func(ctx context.Context) error {
		return s.stream.Start(ctx, "", 1, -1)
	}
}

// 返回写操作的构造函数
func (s *PolicyService) WriteFactory() func(context.Context) error {
	return s.exporter.Start
}

package service

import (
	"context"
	"time"

	exporter_base "github.com/wwqdrh/datamanager/domain/exporter/service/base"
	"github.com/wwqdrh/datamanager/domain/exporter/service/leveldblog"
	stream_base "github.com/wwqdrh/datamanager/domain/stream/service/base"
	"github.com/wwqdrh/datamanager/domain/stream/service/triggerstream"
	"github.com/wwqdrh/datamanager/domain/stream/vo"
	"github.com/wwqdrh/datamanager/pkg"
)

// 传递各类策略

var PolicyService *policyService

type policyService struct {
	stream   stream_base.ITriggerStream
	exporter exporter_base.IExporter
}

// 根据不同的策略选择不同的
func InitPolicyService(streamStr, exporterStr string) *policyService {
	var stream stream_base.ITriggerStream
	var exporter exporter_base.IExporter

	switch streamStr {
	default:
		stream = triggerstream.NewTriggerStream()
	}
	switch exporterStr {
	default:
		exporter = leveldblog.NewLeveldbLog()
	}

	PolicyService = &policyService{
		stream:   stream,
		exporter: exporter,
	}
	return PolicyService
}

// 初始化stream以及exporter
func (s *policyService) Initial(tables ...pkg.TablePolicy) error {
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

func (s *policyService) RegisterTable(table interface{}, min_log_num, outdate int, fields []string, ignoreFields []string) error {
	return s.stream.AddPolicy(vo.TablePolicy{
		Table:        table,
		MinLogNum:    min_log_num,
		Outdate:      outdate,
		SenseFields:  fields,
		IgnoreFields: ignoreFields,
	})
}

func (s *policyService) RemoveTable(table string) error {
	return s.stream.RemovePolicy(table)
}

func (s *policyService) ListTableAllLog(tableName string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	return s.exporter.GetAllLogger(tableName, startTime, endTime, page, pageSize)
}

func (s *policyService) ListTableByTime(tableName string, recordID string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	return s.exporter.GetLoggerByID(tableName, recordID, startTime, endTime, page, pageSize)
}

func (s *policyService) ModifyPolicy(tableName string, args map[string]interface{}) error {
	return s.stream.ModifyPolicy(tableName, args)
}

func (s *policyService) ListTableField(tableName string) []*vo.Fields {
	return s.stream.ListTableField(tableName)
}

// 返回读取操作的 构造函数
func (s *policyService) LoadFactory() func(context.Context) error {
	return func(ctx context.Context) error {
		return s.stream.Start(ctx, "", 1, -1)
	}
}

// 返回写操作的构造函数
func (s *policyService) WriteFactory() func(context.Context) error {
	return s.exporter.Start
}

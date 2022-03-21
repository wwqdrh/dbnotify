package leveldblog

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/wwqdrh/datamanager/domain/exporter/repository"
	"github.com/wwqdrh/datamanager/domain/exporter/service/base"
	"github.com/wwqdrh/datamanager/internal/datautil"
)

type ILeveldbLog interface {
	base.IExpoterInitial
	base.IExporterLog
	base.IExporterAlias
}

type leveldbLog struct {
	ctx context.Context
	ch  chan map[string]interface{}
	base.IExporterAlias
}

func NewLeveldbLog() ILeveldbLog {
	return &leveldbLog{}
}

func (l *leveldbLog) Install(ctx context.Context, ch chan map[string]interface{}, handler base.IExporterAlias) error {
	l.ch = ch
	l.ctx = ctx
	l.IExporterAlias = handler
	return nil
}

func (l *leveldbLog) Start() error {
	for {
		select {
		case <-l.ctx.Done():
			return errors.New("ctx退出")
		case val := <-l.ch:
			// tablename, outdate, minlognum, datar
			datar := val["data"].([]map[string]interface{})
			if err := repository.LogRepoV2.Write(val["TableName"].(string), datar, val["Outdate"].(int), val["MinLogNum"].(int)); err != nil {
				return err
			}
			return nil
		}
	}
}

func (l *leveldbLog) GetAllLogger(tableName string, primaryFields []string, start, end *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	res, err := repository.LogRepoV2.SearchAllRecord(tableName, primaryFields, start, end, page, pageSize)
	if err != nil {
		return nil, err
	}
	result := []map[string]interface{}{}
	for _, item := range res {
		result = append(result, map[string]interface{}{
			"primary": l.TransPrimary(tableName, strings.Split(item["key"].(string), "@")[1]),
			"log":     item["data"],
		})
	}

	return result, nil
}

func (l *leveldbLog) GetLoggerByTime(tableName string, primaryID string, startTime, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	res, err := repository.LogRepoV2.SearchRecordByField(tableName, primaryID, startTime, endTime, page, pageSize)
	if err != nil {
		return nil, err
	}
	result := []map[string]interface{}{}
	for _, item := range res {
		log := l.TransField(tableName, item["data"].([]map[string]interface{}))
		for i, c := range log {
			t, err := datautil.UnParseTime(c["time"].(string))
			if err != nil {
				continue
			}

			log[i]["relations"] = l.getRelationLogger(tableName, primaryID, t)
		}

		result = append(result, map[string]interface{}{
			"primary": l.TransPrimary(tableName, strings.Split(item["key"].(string), "@")[1]),
			"log":     log,
		})
	}

	return result, nil
}

// GetRelationLogger 获取与当前记录有关联的记录 值相同 时间戳差值不超过1分钟
func (l *leveldbLog) getRelationLogger(tableName string, curVal interface{}, stamp time.Time) interface{} {
	return nil
	// 1、获取relations
	// var policy *stream_entity.Policy
	// if val, ok := MetaService.AllPolicy.Load(tableName); !ok {
	// 	return errors.New("未发现当前表的策略")
	// } else {
	// 	policy = val.(*stream_entity.Policy)
	// }
	// // 2、根据relations中的表名，主键字段
	// start, end := stamp.Add(1*time.Second), stamp.Add(-1*time.Second) // 默认在左右一秒范围内的
	// records := []map[string]interface{}{}
	// for _, item := range strings.Split(policy.Relations, ";") {
	// 	var tName, fName string
	// 	{
	// 		t := strings.Split(item, ".")
	// 		tName, fName = t[0], t[1]
	// 	}
	// 	var tpolicy *stream_entity.Policy
	// 	if val, ok := MetaService.AllPolicy.Load(tName); !ok {
	// 		return errors.New("未发现当前表的策略")
	// 	} else {
	// 		tpolicy = val.(*stream_entity.Policy)
	// 	}
	// 	// cond := s.ParsePrimaryVal(curVal.(string))
	// 	cond := map[string]string{
	// 		fName: strings.Split(curVal.(string), "=")[1],
	// 	}

	// 	primaryFields := strings.Split(tpolicy.PrimaryFields, ",")
	// 	data, err := exporter_repo.LogRepoV2.SearchRecordWithCondition(tName, primaryFields, cond, &start, &end, 0, 0)
	// 	if err != nil {
	// 		continue
	// 	}
	// 	records = append(records, data...)
	// }
	// return records
}

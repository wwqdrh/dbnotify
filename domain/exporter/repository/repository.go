package repository

import (
	"fmt"

	"github.com/wwqdrh/datamanager/runtime"

	"github.com/wwqdrh/datamanager/domain/exporter/entity"
)

var (
	LogLocalLogRepo  *entity.LocalLog
	LogRepoV2        *entity.LocalLog2
	FieldMappingRepo *fieldMappingRepo
	R                = runtime.Runtime
)

type fieldMappingRepo struct {
}

func init() {
	LogLocalLogRepo = &entity.LocalLog{}
	LogRepoV2 = &entity.LocalLog2{}
	FieldMappingRepo = &fieldMappingRepo{}
}

func (r fieldMappingRepo) ListAllFieldMapping(tableName string) map[string]string {
	f := new(entity.FieldMapping)
	val, err := f.ListAll(R.GetLogDB(), tableName)
	res := map[string]string{}
	if err != nil {
		return res
	}
	for _, item := range val {
		res[item.FieldID] = item.FieldName
	}
	return res
}

func (r fieldMappingRepo) UpdateAllFieldByMapping(tableName string, records map[string]string) error {
	for key, val := range records {
		cur := &entity.FieldMapping{FieldID: key, FieldName: val}
		if err := cur.Write(R.GetLogDB(), tableName); err != nil {
			fmt.Println(err)
		}
	}
	return nil
}

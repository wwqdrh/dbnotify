package repository

import (
	"gorm.io/gorm"

	"github.com/wwqdrh/datamanager/domain/stream/entity"
	"github.com/wwqdrh/datamanager/runtime"
)

var (
	PolicyRepo   entity.Policy  = entity.Policy{} // repository
	VersionRepo  entity.Version = entity.Version{}
	LogTableRepo *LogTableRepository
	R            = runtime.Runtime
)

type LogTableRepository struct {
	db           *gorm.DB
	logTableName string
}

func InitRepo() {
	LogTableRepo = &LogTableRepository{
		logTableName: R.GetConfig().TempLogTable,
		db:           R.GetDB().DB,
	}
}

// ReadAndDelete
func (r LogTableRepository) ReadAndDeleteByID(tableName string, num int) ([]*entity.LogTable, error) {
	var res []*entity.LogTable
	if err := r.db.Table(r.logTableName).Where("table_name = ?", tableName).Where("id > ?", 0).Limit(num).Find(&res).Error; err != nil {
		return nil, err
	}

	ids := []uint64{}
	for _, item := range res {
		ids = append(ids, item.ID)
	}
	if len(ids) > 0 {
		if err := r.db.Table(r.logTableName).Delete(&entity.LogTable{}, ids).Error; err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (r LogTableRepository) ReadBytableNameAndLimit(tableName string, id uint64, num int) ([]*entity.LogTable, error) {
	var res []*entity.LogTable
	if err := r.db.Table(r.logTableName).Where("table_name = ?", tableName).Where("id > ?", id).Limit(num).Find(&res).Error; err != nil {
		return nil, err
	}
	return res, nil
}

func (r LogTableRepository) DeleteByID(data []*entity.LogTable) error {
	if len(data) == 0 {
		return nil
	}
	ids := []uint64{}
	for _, item := range data {
		ids = append(ids, item.ID)
	}
	return r.db.Table(r.logTableName).Delete(&entity.LogTable{}, ids).Error
}

package manager

import (
	"datamanager/core/manager/model"
	"datamanager/pkg/plugger/postgres"
	"errors"
	"strings"
	"sync"
	"time"
)

var (
	allPolicy *sync.Map = &sync.Map{} //  map[string]*model.Policy
)

type (
	ManagerCore struct {
		*ManagerConf
	}
)

func GetAllPolicy() *sync.Map {
	return allPolicy
}

func NewManagerCore(conf *ManagerConf) *ManagerCore {
	return &ManagerCore{
		ManagerConf: conf,
	}
}

// 自动处理需要的表结构
func (m *ManagerCore) AutoMigrate() {
	model.PolicyRepo.Migrate(m.VersionDB.DB)
	// 读取策略
	for _, item := range model.PolicyRepo.GetAllData(m.VersionDB.DB) {
		allPolicy.Store(item.TableName, item)
	}
	// 根据策略再继续生成对应的版本数据表
	allPolicy.Range(func(key, value interface{}) bool {
		item := value.(*model.Policy)
		model.VersionRepo.MigrateWithName(m.VersionDB.DB, item.TableName+"_version")
		return true
	})
	// 添加日志表
	m.TargetDB.DB.Table(m.LogTableName).AutoMigrate(model.LogTableRepo)
}

// 注册新的数据表
// 需要对表创建触发器 如果表已经存在触发器了则不要再创建了 操作postgres
// 可以记录下创建触发器的时间
func (m *ManagerCore) Register(table interface{}, run bool) error {
	var (
		policy *model.Policy
	)
	tableName := m.TargetDB.GetTableName(table)
	if tableName == "" {
		return errors.New("表名不能为空")
	}

	if val, ok := allPolicy.Load(tableName); !ok {
		primaryFields := strings.Join(m.TargetDB.GetPrimary(table), ",")
		policy = &model.Policy{TableName: tableName, PrimaryFields: primaryFields, Outdate: m.OutDate}

		if err := model.PolicyRepo.CreateNoExist(
			m.VersionDB.DB, policy,
			map[string]interface{}{"table_name": tableName},
		); err != nil {
			return err
		}
		allPolicy.Store(tableName, policy)
	} else {
		policy = val.(*model.Policy)
	}

	m.TargetDB.DB.AutoMigrate(table)
	m.TargetDB.Trigger.CreateTrigger(
		m.TriggerPolicy,
		tableName,
		[]postgres.TriggerType{postgres.ALL},
	)
	m.LoggerPolicy.SetSenseFields(tableName, strings.Split(policy.Fields, ","))

	if run {
		m.LoggerPolicy.Run(tableName, strings.Split(policy.Fields, ","), policy.Outdate)
	}

	return nil
}

// 获取监听的所有数据表的名字
func (m *ManagerCore) ListTable() []string {
	res := []string{}
	allPolicy.Range(func(key, value interface{}) bool {
		item := value.(*model.Policy)
		res = append(res, item.TableName)
		return true
	})
	return res
}

// ListTableLog 从postgres中获取 获取指定表的所有历史记录
func (m *ManagerCore) ListTableLog(tableName string, startTime *time.Time, endTime *time.Time) ([]*model.LogTable, error) {
	res := []*model.LogTable{}

	if startTime == nil {
		if err := m.TargetDB.DB.Table(m.LogTableName).Where("table_name = ?", tableName).Find(&res).Error; err != nil {
			return nil, err
		}
	} else if endTime == nil {
		if err := m.TargetDB.DB.Where("table_name = ?", tableName).Where("time > ?", startTime).Find(&res).Error; err != nil {
			return nil, err
		}
	} else {
		if err := m.TargetDB.DB.Where("table_name = ?", tableName).Where("time > ?", startTime).Where("time < ", endTime).Find(&res).Error; err != nil {
			return nil, err
		}
	}

	return res, nil
}

// 从leveldb中获取
func (m *ManagerCore) ListTableLog2(tableName string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	if val, ok := allPolicy.Load(tableName); !ok {
		return nil, errors.New(tableName + "未进行监听")
	} else {
		policy := val.(*model.Policy)
		fields := strings.Split(policy.Fields, ",")
		return m.LoggerPolicy.GetLogger(tableName+"_log.db", fields, startTime, endTime, page, pageSize)
	}
}

// 修改表的trigger
func (m *ManagerCore) ModifyOutdate(tableName string, outdate int) error {
	var policy *model.Policy
	if val, ok := allPolicy.Load(tableName); !ok {
		return errors.New("表未注册")
	} else {
		policy = val.(*model.Policy)
	}
	if policy.Outdate == outdate {
		return nil
	}

	m.TriggerPolicy.UpdateConfig(map[string]interface{}{"outdate": outdate})
	policy.Outdate = outdate
	if err := m.VersionDB.DB.Save(policy).Error; err != nil {
		return err
	}

	return nil
}

// 修改表的敏感字段 不需要检查是否有这些字段
func (m *ManagerCore) ModifyField(tableName string, fields []string) error {
	var policy *model.Policy
	if val, ok := allPolicy.Load(tableName); !ok {
		return errors.New("表未注册")
	} else {
		policy = val.(*model.Policy)
	}
	policy.Fields = strings.Join(fields, ",")
	m.LoggerPolicy.SetSenseFields(tableName, fields)
	return m.VersionDB.DB.Save(policy).Error
}

func (m *ManagerCore) ListTableByName2(tableName string, fields []string, startTime, endTime *time.Time, page, pageSize int) (interface{}, error) {
	if _, ok := allPolicy.Load(tableName); !ok {
		return nil, errors.New("表未注册")
	}
	return m.LoggerPolicy.GetLogger(tableName+"_log.db", fields, startTime, endTime, page, pageSize)
}

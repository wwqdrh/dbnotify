package manager

import (
	"datamanager/core/manager/model"
	"datamanager/pkg/plugger/postgres"
	"errors"
	"strings"
	"time"
)

var (
	allPolicy map[string]*model.Policy = make(map[string]*model.Policy)
)

type (
	ManagerCore struct {
		*ManagerConf
	}
)

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
		allPolicy[item.TableName] = item
	}

	// 根据策略再继续生成对应的版本数据表
	for _, item := range allPolicy {
		model.VersionRepo.MigrateWithName(m.VersionDB.DB, item.TableName+"_version")
	}

	// 添加日志表
	m.TargetDB.DB.Table(m.LogTableName).AutoMigrate(model.LogTableRepo)
}

// 注册新的数据表
// 需要对表创建触发器 如果表已经存在触发器了则不要再创建了 操作postgres
// 可以记录下创建触发器的时间
func (m *ManagerCore) Register(table interface{}) error {
	tableName := m.TargetDB.GetTableName(table)
	if tableName == "" {
		return errors.New("表名不能为空")
	}
	if _, ok := allPolicy[tableName]; !ok {
		// versionRepo.MigrateWithName(m.versionDB, tableName+"_version")
		// 添加policy策略 首先需要判断table_name是否存在
		policy := &model.Policy{TableName: tableName, Outdate: m.OutDate}
		if err := model.PolicyRepo.CreateNoExist(
			m.VersionDB.DB, policy,
			map[string]interface{}{"table_name": tableName},
		); err != nil {
			return err
		}
		allPolicy[tableName] = policy
	}

	// m.TriggerPolicy = trigger.NewDymaTriggerPolicy(m.TargetDB, m.LogTableName, table, allPolicy[tableName].Outdate)
	m.TargetDB.DB.AutoMigrate(table)
	m.TargetDB.Trigger.CreateTrigger(
		m.TriggerPolicy,
		tableName,
		[]postgres.TriggerType{postgres.ALL},
	)
	m.LoggerPolicy.SetSenseFields(tableName, strings.Split(allPolicy[tableName].Fields, ","))

	m.LoggerPolicy.Run(tableName, strings.Split(allPolicy[tableName].Fields, ","), allPolicy[tableName].Outdate)

	return nil
}

// 获取监听的所有数据表的名字
func (m *ManagerCore) ListTable() []string {
	res := []string{}
	for _, item := range allPolicy {
		res = append(res, item.TableName)
	}
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
	fields := strings.Split(allPolicy[tableName].Fields, ",")
	return m.LoggerPolicy.GetLogger(tableName+"_log.db", fields, startTime, endTime, page, pageSize)
}

// 将数据库中的日志数据格式转换成通用的
func (m *ManagerCore) AdapterLog(logData []*model.LogTable, fields ...string) (interface{}, error) {
	return m.LoggerPolicy.Dump(logData, fields...)
}

// 修改表的trigger
func (m *ManagerCore) ModifyTrigger(tableName string, outdate int) error {
	if _, ok := allPolicy[tableName]; !ok {
		return errors.New("表未注册")
	}
	if allPolicy[tableName].Outdate == outdate {
		return nil
	}

	// if err := m.TargetDB.Trigger.DeleteTrigger(m.TriggerPolicy, tableName, postgres.BEFORE_UPDATE); err != nil {
	// 	return err
	// }
	m.TriggerPolicy.UpdateConfig(map[string]interface{}{"outdate": outdate})
	allPolicy[tableName].Outdate = outdate
	if err := m.VersionDB.DB.Save(allPolicy[tableName]).Error; err != nil {
		return err
	}
	// if err := m.TargetDB.Trigger.CreateTrigger(
	// 	m.TriggerPolicy,
	// 	tableName,
	// 	[]postgres.TriggerType{postgres.BEFORE_UPDATE},
	// ); err != nil {
	// 	return errors.New("创建trigger失败")
	// }

	return nil
}

// 修改表的敏感字段 不需要检查是否有这些字段
func (m *ManagerCore) ModifyField(tableName string, fields []string) error {
	if _, ok := allPolicy[tableName]; !ok {
		return errors.New("表未注册")
	}
	policy := allPolicy[tableName]
	policy.Fields = strings.Join(fields, ",")
	m.LoggerPolicy.SetSenseFields(tableName, fields)
	return m.VersionDB.DB.Save(policy).Error
}

// 根据表名和字段名查找整个历史记录中所关心的字段发生了哪些变化
// func (m *ManagerCore) ListTableByName(tableName string, fieldNames []string, page, pageSize int) (interface{}, error) {
// 	logs, err := m.ListTableLog(tableName, nil, nil)
// 	if err != nil {
// 		return nil, err
// 	}
// 	data, err := m.AdapterLog(logs, fieldNames...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return data, nil
// }

func (m *ManagerCore) ListTableByName2(tableName string, fields []string, startTime, endTime *time.Time, page, pageSize int) (interface{}, error) {
	return m.LoggerPolicy.GetLogger(tableName+"_log.db", fields, startTime, endTime, page, pageSize)
}

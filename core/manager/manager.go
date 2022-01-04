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

func GetAllPolicy() map[string]*model.Policy {
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
		primaryFields := strings.Join(m.TargetDB.GetPrimary(table), ",")
		policy := &model.Policy{TableName: tableName, PrimaryFields: primaryFields, Outdate: m.OutDate}
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

// 从leveldb中获取
func (m *ManagerCore) ListTableLog2(tableName string, startTime *time.Time, endTime *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	if _, ok := allPolicy[tableName]; !ok {
		return nil, errors.New("表未注册")
	}
	fields := strings.Split(allPolicy[tableName].Fields, ",")
	return m.LoggerPolicy.GetLogger(tableName+"_log.db", fields, startTime, endTime, page, pageSize)
}

// 修改表的trigger
func (m *ManagerCore) ModifyOutdate(tableName string, outdate int) error {
	if _, ok := allPolicy[tableName]; !ok {
		return errors.New("表未注册")
	}
	if allPolicy[tableName].Outdate == outdate {
		return nil
	}

	m.TriggerPolicy.UpdateConfig(map[string]interface{}{"outdate": outdate})
	allPolicy[tableName].Outdate = outdate
	if err := m.VersionDB.DB.Save(allPolicy[tableName]).Error; err != nil {
		return err
	}

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

func (m *ManagerCore) ListTableByName2(tableName string, fields []string, startTime, endTime *time.Time, page, pageSize int) (interface{}, error) {
	if _, ok := allPolicy[tableName]; !ok {
		return nil, errors.New("表未注册")
	}
	return m.LoggerPolicy.GetLogger(tableName+"_log.db", fields, startTime, endTime, page, pageSize)
}

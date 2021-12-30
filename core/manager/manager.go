package manager

import (
	"datamanager/core/manager/logger"
	"datamanager/core/manager/model"
	"datamanager/core/manager/trigger"
	"datamanager/pkg/plugger/postgres"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
)

var (
	policyRepo  model.Policy  = model.Policy{} // repository
	versionRepo model.Version = model.Version{}

	allPolicy map[string]*model.Policy = make(map[string]*model.Policy)
)

type (
	ManagerCore struct {
		targetDB      *postgres.PostgresDriver
		versionDB     *gorm.DB
		loggerPolicy  logger.ILogger
		triggerPolicy postgres.ITriggerPolicy
		logTableName  string
	}
)

func NewManagerCore(targetDB *postgres.PostgresDriver, versionDB *gorm.DB) *ManagerCore {
	return &ManagerCore{
		logTableName: "data_version_log",
		targetDB:     targetDB,
		versionDB:    versionDB,
		loggerPolicy: logger.NewDefaultLogger(),
	}
}

// 自动构建表结构 policy表
func (m *ManagerCore) AutoMigrate() {
	policyRepo.Migrate(m.versionDB)
	// 读取策略
	for _, item := range policyRepo.GetAllData(m.versionDB) {
		allPolicy[item.TableName] = item
	}

	// 根据策略再继续生成对应的版本数据表
	for _, item := range allPolicy {
		versionRepo.MigrateWithName(m.versionDB, item.TableName+"_version")
	}
}

// 注册新的数据表
// 需要对表创建触发器 如果表已经存在触发器了则不要再创建了 操作postgres
// 可以记录下创建触发器的时间
func (m *ManagerCore) Register(table interface{}) error {
	stmt := &gorm.Statement{DB: m.versionDB}
	stmt.Parse(table)
	tableName := stmt.Schema.Table
	if tableName == "" {
		return errors.New("表名不能为空")
	}
	if _, ok := allPolicy[tableName]; !ok {
		versionRepo.MigrateWithName(m.versionDB, tableName+"_version")
		// 添加policy策略 首先需要判断table_name是否存在
		policy := &model.Policy{TableName: tableName, Outdate: 15}
		if err := policyRepo.CreateNoExist(
			m.versionDB, policy,
			map[string]interface{}{"table_name": tableName},
		); err != nil {
			return err
		}
		allPolicy[tableName] = policy
	}
	// m.targetDB.Trigger.CreateTrigger(
	// 	trigger.NewTriggerPolicy(m.targetDB, table),
	// 	[]postgres.TriggerType{postgres.BEFORE_UPDATE, postgres.AFTER_UPDATE, postgres.AFTER_CREATE, postgres.BEFORE_DELETE},
	// )

	m.triggerPolicy = trigger.NewDymaTriggerPolicy(m.targetDB, m.logTableName, table, allPolicy[tableName].Outdate)
	m.targetDB.Trigger.CreateTrigger(
		m.triggerPolicy,
		tableName,
		[]postgres.TriggerType{postgres.ALL},
	)
	m.loggerPolicy.SetSenseFields(strings.Split(allPolicy[tableName].Fields, ","))

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

// ListTableLog 获取指定表的所有历史记录
func (m *ManagerCore) ListTableLog(tableName string, startTime *time.Time, endTime *time.Time) ([]*trigger.LogTable, error) {
	res := []*trigger.LogTable{}

	if startTime == nil {
		if err := m.targetDB.DB.Table(m.logTableName).Where("table_name = ?", tableName).Find(&res).Error; err != nil {
			return nil, err
		}
	} else if endTime == nil {
		if err := m.targetDB.DB.Where("table_name = ?", tableName).Where("time > ?", startTime).Find(&res).Error; err != nil {
			return nil, err
		}
	} else {
		if err := m.targetDB.DB.Where("table_name = ?", tableName).Where("time > ?", startTime).Where("time < ", endTime).Find(&res).Error; err != nil {
			return nil, err
		}
	}

	return res, nil
}

// 将数据库中的日志数据格式转换成通用的
func (m *ManagerCore) AdapterLog(logData []*trigger.LogTable, fields ...string) (interface{}, error) {
	return m.loggerPolicy.Dump(logData, fields...)
}

// 修改表的trigger
func (m *ManagerCore) ModifyTrigger(tableName string, outdate int) error {
	if _, ok := allPolicy[tableName]; !ok {
		return errors.New("表未注册")
	}
	if allPolicy[tableName].Outdate == outdate {
		return nil
	}

	if err := m.targetDB.Trigger.DeleteTrigger(m.triggerPolicy, tableName, postgres.BEFORE_UPDATE); err != nil {
		return err
	}
	m.triggerPolicy.UpdateConfig(map[string]interface{}{"outdate": outdate})
	allPolicy[tableName].Outdate = outdate
	if err := m.versionDB.Save(allPolicy[tableName]).Error; err != nil {
		return err
	}
	if err := m.targetDB.Trigger.CreateTrigger(
		m.triggerPolicy,
		tableName,
		[]postgres.TriggerType{postgres.BEFORE_UPDATE},
	); err != nil {
		return errors.New("创建trigger失败")
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
	m.loggerPolicy.SetSenseFields(fields)
	return m.versionDB.Save(policy).Error
}

// 根据表名和字段名查找整个历史记录中所关心的字段发生了哪些变化
func (m *ManagerCore) ListTableByName(tableName string, fieldNames ...string) (interface{}, error) {
	logs, err := m.ListTableLog(tableName, nil, nil)
	if err != nil {
		return nil, err
	}
	data, err := m.AdapterLog(logs, fieldNames...)
	if err != nil {
		return nil, err
	}
	return data, nil
	// [
	// 	{
	// 		"action": "update",
	// 		"log": {
	// 			"data": {
	// 				"age": {
	// 					"after": 20,
	// 					"before": 18
	// 				}
	// 			}
	// 		},
	// 		"time": "2021-12-30T14:50:39.80038+08:00"
	// 	},
	// 	{
	// 		"action": "update",
	// 		"log": {
	// 			"data": {}
	// 		},
	// 		"time": "2021-12-30T14:51:00.795+08:00"
	// 	},
	// 	{
	// 		"action": "delete",
	// 		"log": {
	// 			"data": {
	// 				"age": 20,
	// 				"name": "saan"
	// 			}
	// 		},
	// 		"time": "2021-12-30T16:14:31.751899+08:00"
	// 	}
	// ],
}
